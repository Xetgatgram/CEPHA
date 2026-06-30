package com.covertchannel.framework.kafka;

import com.covertchannel.producer.KafkaPacketProducer;
import com.covertchannel.producer.PcapFileReader;
import org.pcap4j.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Service to manage Kafka Producer capture sessions in-process
 * Thread-safe with volatile state and synchronized mutating methods
 */
@Service
public class ProducerService {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerService.class);

    @Value("${kafka.bootstrap-servers:localhost:9092}")
    private String kafkaBrokers;

    @Value("${kafka.producer.pcap-upload-dir:/tmp/pcap-uploads}")
    private String pcapUploadDir;

    // Thread pool for capture tasks
    private final ExecutorService captureExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r);
        t.setName("pcap-capture-worker");
        t.setDaemon(true);
        return t;
    });

    // State management
    private volatile CaptureState state = CaptureState.IDLE;
    private volatile PcapHandle activeHandle;
    private volatile KafkaPacketProducer activeProducer;
    private volatile String currentMode;
    private volatile String currentSource;
    private volatile String currentTopic;
    private volatile String currentFilter;
    private volatile Future<?> captureTask;
    private final AtomicBoolean cleanupDone = new AtomicBoolean(false);

    private enum CaptureState {
        IDLE, RUNNING, STOPPING
    }

    /**
     * Start live network capture with optional BPF filter
     */
    public synchronized void startLiveCapture(String interfaceName, String topic, String bpfFilter) throws Exception {
        if (state != CaptureState.IDLE) {
            throw new IllegalStateException("Producer is already running (state: " + state + ")");
        }

        LOG.info("Starting live capture: interface={}, topic={}, filter={}", 
                 interfaceName, topic, bpfFilter != null ? bpfFilter : "(none)");

        PcapHandle handle = null;
        KafkaPacketProducer producer = null;

        try {
            // Find and open network interface
            PcapNetworkInterface nif = "any".equalsIgnoreCase(interfaceName) 
                ? Pcaps.getDevByName("any") 
                : Pcaps.getDevByName(interfaceName);

            if (nif == null) {
                throw new IllegalArgumentException("Network interface not found: " + interfaceName);
            }

            // Open handle in promiscuous mode
            handle = nif.openLive(65536, PcapNetworkInterface.PromiscuousMode.PROMISCUOUS, 10);

            // Apply BPF filter if provided  pcap4j validates it
            if (bpfFilter != null && !bpfFilter.trim().isEmpty()) {
                try {
                    handle.setFilter(bpfFilter.trim(), BpfProgram.BpfCompileMode.OPTIMIZE);
                    LOG.info("BPF filter applied: {}", bpfFilter);
                } catch (PcapNativeException e) {
                    handle.close();
                    throw new IllegalArgumentException("Invalid BPF filter: " + e.getMessage(), e);
                }
            }

            // Initialize Kafka producer
            producer = new KafkaPacketProducer(kafkaBrokers, topic);

            // Assign to fields only after successful initialization
            activeHandle = handle;
            activeProducer = producer;
            currentMode = "LIVE";
            currentSource = interfaceName;
            currentTopic = topic;
            currentFilter = bpfFilter;
            cleanupDone.set(false);
            state = CaptureState.RUNNING;

            // Start capture loop in background thread
            final PcapHandle finalHandle = handle;
            final KafkaPacketProducer finalProducer = producer;

            captureTask = captureExecutor.submit(() -> {
                try {
                    LOG.info("Capture loop started");
                    finalHandle.loop(-1, (PacketListener) packet -> {
                        try {
                            long timestamp = finalHandle.getTimestamp().getTime();
                            int dlt = finalHandle.getDlt().value();
                            finalProducer.sendPacketAsync(packet, dlt, timestamp);
                        } catch (Exception e) {
                            LOG.warn("Failed to send packet to Kafka: {}", e.getMessage());
                        }
                    });
                } catch (InterruptedException e) {
                    LOG.info("Capture interrupted");
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    LOG.error("Capture failed", e);
                } finally {
                    cleanupCaptureResources();
                }
            });

            LOG.info("Live capture started successfully");

        } catch (Exception e) {
            // Cleanup on failure before fields are assigned
            if (producer != null) {
                try { producer.close(); } catch (Exception ignored) {}
            }
            if (handle != null) {
                try { handle.close(); } catch (Exception ignored) {}
            }
            state = CaptureState.IDLE;
            throw e;
        }
    }

    /**
     * Start file based packet production
     */
    public synchronized void startFileProduction(String pcapFilePath, String topic, int maxPackets) throws Exception {
        if (state != CaptureState.IDLE) {
            throw new IllegalStateException("Producer is already running (state: " + state + ")");
        }

        // Validate path to prevent traversal
        File pcapFile = validatePcapFilePath(pcapFilePath);

        LOG.info("Starting file production: file={}, topic={}, maxPackets={}", pcapFile.getPath(), topic, maxPackets);

        // Store metadata
        currentMode = "FILE";
        currentSource = pcapFile.getPath();
        currentTopic = topic;
        cleanupDone.set(false);
        state = CaptureState.RUNNING;

        // Submit file processing task using PcapFileReader
        try {
            captureTask = captureExecutor.submit(() -> {
                KafkaPacketProducer producer = null;
                try {
                    producer = new KafkaPacketProducer(kafkaBrokers, topic);
                    // Use canonical path from validation
                    PcapFileReader reader = new PcapFileReader(pcapFile.getPath(), producer, maxPackets);
                    PcapFileReader.ProcessingResult result = reader.process();
                    
                    LOG.info("✓ File processing completed: {} packets sent, {} failed", 
                             result.getPacketsSent(), result.getPacketsFailed());
                    
                } catch (Exception e) {
                    LOG.error("File processing failed", e);
                } finally {
                    // Cleanup without holding monito avoids deadlock
                    if (producer != null) {
                        try { producer.close(); } catch (Exception e) {
                            LOG.warn("Error closing producer: {}", e.getMessage());
                        }
                    }
                    // Direct volatile writes no lock needed
                    currentMode = null;
                    currentSource = null;
                    currentTopic = null;
                    state = CaptureState.IDLE;
                }
            });
        } catch (RejectedExecutionException e) {
            // reset state
            state = CaptureState.IDLE;
            currentMode = null;
            currentSource = null;
            currentTopic = null;
            throw new IllegalStateException("Capture executor is not available", e);
        }

        LOG.info("File production started");
    }

    /**
     * Validate PCAP file path to prevent directory traversal
     * Rejects absolute paths and ensures canonical path is within base directory
     */
    private File validatePcapFilePath(String relativePath) throws IOException {
        // Reject absolute paths explicitly
        if (new File(relativePath).isAbsolute()) {
            throw new IllegalArgumentException("Absolute paths are not allowed");
        }

        File baseDir = new File(pcapUploadDir).getCanonicalFile();
        File targetFile = new File(baseDir, relativePath).getCanonicalFile();

        if (!targetFile.getPath().startsWith(baseDir.getPath() + File.separator)) {
            throw new IllegalArgumentException("Invalid file path: directory traversal detected");
        }

        if (!targetFile.exists()) {
            throw new IllegalArgumentException("PCAP file not found: " + relativePath);
        }

        return targetFile;
    }

    /**
     * Stop the currently running producer
     */
    public synchronized void stopProducer() {
        if (state == CaptureState.IDLE) {
            throw new IllegalStateException("No producer is currently running");
        }

        if (state == CaptureState.STOPPING) {
            LOG.warn("Stop already in progress");
            return;
        }

        state = CaptureState.STOPPING;
        LOG.info("Stopping producer...");

        try {
            // Break pcap loop send signal to handle.loop()
            PcapHandle handle = activeHandle;
            if (handle != null) {
                try {
                    handle.breakLoop();
                    LOG.debug("Sent break signal to pcap handle");
                } catch (NotOpenException e) {
                    LOG.debug("Handle already closed");
                } catch (Exception e) {
                    LOG.warn("Failed to break pcap loop: {}", e.getMessage());
                }
            }

            // Wait for graceful shutdown
            Future<?> task = captureTask;
            if (task != null && !task.isDone()) {
                try {
                    task.get(3, TimeUnit.SECONDS);
                    LOG.debug("Capture task completed gracefully");
                } catch (TimeoutException e) {
                    LOG.warn("Capture task didn't complete within timeout, canceling");
                    task.cancel(true);
                } catch (InterruptedException e) {
                    task.cancel(true);
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    LOG.debug("Capture task completed with exception: {}", e.getMessage());
                }
            }

            // Final cleanup only if not already done by task
            // After task.get() returns, task finally has completed, so cleanupDone is reliable
            cleanupCaptureResources();
            
            LOG.info("Producer stopped");

        } finally {
            state = CaptureState.IDLE;
        }
    }

    /**
     * Cleanup capture resources
     * Called from tasks finally block OR from stopProducer()
     * Correctness  stopProducer() calls task.get() before this ensuring tasks finally completes first
     */
    private void cleanupCaptureResources() {
        // Atomic check-and-set to prevent double execution
        if (!cleanupDone.compareAndSet(false, true)) {
            return; // Already cleaned up
        }

        // Close pcap handle use local variable to avoid TOCTOU race
        PcapHandle handle = activeHandle;
        activeHandle = null;
        if (handle != null) {
            try {
                handle.close();
                LOG.debug("Pcap handle closed");
            } catch (Exception e) {
                LOG.warn("Error closing pcap handle: {}", e.getMessage());
            }
        }

        // Close Kafka producer flush pending messages
        KafkaPacketProducer producer = activeProducer;
        activeProducer = null;
        if (producer != null) {
            try {
                producer.close();
                LOG.debug("Kafka producer closed");
            } catch (Exception e) {
                LOG.warn("Error closing Kafka producer: {}", e.getMessage());
            }
        }

        // Clear metadata
        currentMode = null;
        currentSource = null;
        currentTopic = null;
        currentFilter = null;
    }

    /**
     * Get producer status threadsafe volatile reads with local copies
     */
    public Map<String, Object> getStatus() {
        // Local copies to ensure consistent snapshot
        CaptureState currentState = state;
        String mode = currentMode;
        String source = currentSource;
        String topic = currentTopic;
        String filter = currentFilter;
        
        Map<String, Object> status = new HashMap<>();
        status.put("running", currentState == CaptureState.RUNNING);
        status.put("state", currentState.name());
        status.put("mode", mode);
        status.put("source", source);
        status.put("topic", topic);
        
        if (filter != null && !filter.isEmpty()) {
            status.put("filter", filter);
        }
        
        return status;
    }

    /**
     * List available network interfaces
     */
    public List<Map<String, String>> listNetworkInterfaces() {
        try {
            List<PcapNetworkInterface> allDevs = Pcaps.findAllDevs();
            return allDevs.stream()
                    .map(dev -> {
                        Map<String, String> info = new HashMap<>();
                        info.put("name", dev.getName());
                        info.put("description", dev.getDescription() != null ? dev.getDescription() : "No description");
                        return info;
                    })
                    .collect(Collectors.toList());
        } catch (PcapNativeException e) {
            LOG.error("Failed to list network interfaces", e);
            return Collections.emptyList();
        }
    }

    /**
     * List available PCAP files
     */
    public List<String> listPcapFiles() {
        try {
            File dir = new File(pcapUploadDir).getCanonicalFile();
            
            if (!dir.exists() || !dir.isDirectory()) {
                return Collections.emptyList();
            }

            File[] files = dir.listFiles((d, name) -> name.endsWith(".pcap") || name.endsWith(".pcapng"));
            if (files == null) {
                return Collections.emptyList();
            }

            return Arrays.stream(files)
                    .map(File::getName)
                    .sorted()
                    .collect(Collectors.toList());
                    
        } catch (IOException e) {
            LOG.error("Failed to list PCAP files: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Spring lifecycle cleanup reuses stopProducer logic
     */
    @PreDestroy
    public void cleanup() {
        LOG.info("Shutting down producer service...");
        
        // Try graceful stop if running
        try {
            if (state != CaptureState.IDLE) {
                stopProducer();
            }
        } catch (IllegalStateException e) {
            // Already stopped, ignore
        }

        // Shutdown executor
        captureExecutor.shutdown();
        try {
            if (!captureExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("Executor didn t terminate gracefully, forcing shutdown");
                captureExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            captureExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        LOG.info("Producer service shutdown complete");
    }
}
