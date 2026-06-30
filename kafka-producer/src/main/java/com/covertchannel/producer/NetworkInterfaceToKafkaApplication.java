package com.covertchannel.producer;

import com.covertchannel.producer.KafkaPacketProducer;
import org.pcap4j.core.*;
import org.pcap4j.packet.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Application that captures packets from a live network interface and sends them to Kafka.
 */
public class NetworkInterfaceToKafkaApplication {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkInterfaceToKafkaApplication.class);
    private static final int SNAPLEN = 65536; // Max packet size
    private static final int READ_TIMEOUT = 10; // in milliseconds
    private static final String DEFAULT_FILTER = ""; // Capture all

    private static volatile boolean running = true;
    private static volatile PcapHandle activeHandle = null;
    private static volatile KafkaPacketProducer activeProducer = null;

    public static void main(String[] args) {
        if (args.length < 3) {
            printUsage();
            System.exit(1);
        }

        String interfaceName = args[0];
        String kafkaBrokers = args[1];
        String kafkaTopic = args[2];


        LOG.info("Starting NetworkInterfaceToKafkaApplication");
        LOG.info("  Interface: {}", interfaceName);
        LOG.info("  Kafka Brokers: {}", kafkaBrokers);
        LOG.info("  Kafka Topic: {}", kafkaTopic);

        registerShutdownHook();

        KafkaPacketProducer kafkaProducer = new KafkaPacketProducer(kafkaBrokers, kafkaTopic);
        activeProducer = kafkaProducer;



        try {
            // 1 Find network interface
            PcapNetworkInterface nif;
            if ("any".equalsIgnoreCase(interfaceName)) {
                nif = Pcaps.getDevByName("any");
            } else {
                nif = Pcaps.getDevByName(interfaceName);
            }

            if (nif == null) {
                LOG.error("Network interface '{}' not found.", interfaceName);
                listInterfaces();
                return;
            }

            LOG.info("Opening interface: {} ({})", nif.getName(), nif.getDescription());

            // 2 Open handle
            PcapHandle handle = nif.openLive(SNAPLEN, PcapNetworkInterface.PromiscuousMode.PROMISCUOUS, READ_TIMEOUT);
            activeHandle = handle;

            try {
                //  Set filter e.g., "tcp port 80"
                if (!DEFAULT_FILTER.isEmpty()) {
                    handle.setFilter(DEFAULT_FILTER, BpfProgram.BpfCompileMode.OPTIMIZE);
                }

                LOG.info("Capture started. Press Ctrl+C to stop gracefully.");

                // 3 Capture loop , graceful shutdown support
                int dlt = handle.getDlt().value();
                while (running) {
                    try {
                        Packet packet = handle.getNextPacket();
                        if (packet == null) {
                            continue; // Timeout check running flag again
                        }

                        long timestamp = handle.getTimestamp().getTime(); //in milliseconds
                        kafkaProducer.sendPacketAsync(packet, dlt, timestamp);

                    } catch (NotOpenException e) {
                        LOG.debug("Handle closed during capture");
                        break;
                    } catch (Exception e) {
                        if (running) {
                            LOG.warn("Failed to process packet: {}", e.getMessage());
                        }
                    }
                }

                LOG.info("Capture loop exited gracefully");

            } catch (NotOpenException e) {
                throw new RuntimeException(e);
            } finally {
                if (handle != null && handle.isOpen()) {
                    handle.close();
                    LOG.debug("Pcap handle closed");
                }
            }

        } catch (PcapNativeException e) {
            LOG.error("Error during live capture: {}", e.getMessage(), e);
        } finally {
            // Cleanup in finally block (also called by shutdown hook)
            cleanup();
        }

        LOG.info("Application finished");
    }


    /**
     * Register shutdown hook for graceful SIGINT/SIGTERM handling
     */
    private static void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received (Ctrl+C), stopping capture...");
            running = false;

            // Break pcap loop if still running
            if (activeHandle != null) {
                try {
                    activeHandle.breakLoop();
                } catch (Exception e) {
                    LOG.debug("Failed to break loop: {}", e.getMessage());
                }
            }

            // Wait briefly for main thread to finish
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Final cleanup
            cleanup();

            LOG.info("Shutdown complete");
        }, "shutdown-hook"));
    }

    /**
     * Cleanup resources idempotent
     */
    private static void cleanup() {
        KafkaPacketProducer producer = activeProducer;
        if (producer != null) {
            activeProducer = null;
            try {
                LOG.info("Closing Kafka producer...");
                producer.close(); // Blocks until buffer is flushed or timeout
                LOG.info("Kafka producer closed");
            } catch (Exception e) {
                LOG.warn("Error closing Kafka producer: {}", e.getMessage());
            }
        }
    }
    private static void listInterfaces() {
        try {
            List<PcapNetworkInterface> allDevs = Pcaps.findAllDevs();
            LOG.info("Available interfaces:");
            for (PcapNetworkInterface dev : allDevs) {
                LOG.info(" - {} : {}", dev.getName(), dev.getDescription() != null ? dev.getDescription() : "No description");
            }
        } catch (PcapNativeException e) {
            LOG.error("Failed to list interfaces", e);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java ...NetworkInterfaceToKafkaApplication <interface-name> <kafka-brokers> <kafka-topic>");
        System.out.println("Use 'any' for all interfaces (Linux only).");
    }
}
