package com.covertchannel.framework.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST API for Kafka Producer control
 */
@RestController
@RequestMapping("/api/kafka/producer")
public class ProducerRestController {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerRestController.class);
    private final ProducerService producerService;

    public ProducerRestController(ProducerService producerService) {
        this.producerService = producerService;
    }

    /**
     * Get producer status
     * GET /api/kafka/producer/status
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        return ResponseEntity.ok(producerService.getStatus());
    }

    /**
     * Start live network capture
     * POST /api/kafka/producer/start-live
     * Body: { "interface": "eth0", "topic": "network-flows" }
     */
    @PostMapping("/start-live")
    public ResponseEntity<Map<String, String>> startLiveCapture(@RequestBody Map<String, String> request) {
        try {
            String interfaceName = request.get("interface");
            String topic = request.get("topic");
            String bpfFilter = request.get("bpfFilter"); // Optional

            if (interfaceName == null || topic == null) {
                throw new IllegalArgumentException("Missing required fields: interface, topic");
            }

            producerService.startLiveCapture(interfaceName, topic, bpfFilter);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Live capture started");
            response.put("interface", interfaceName);
            response.put("topic", topic);
            if (bpfFilter != null && !bpfFilter.trim().isEmpty()) {
                response.put("filter", bpfFilter);
            }

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            LOG.error("Failed to start live capture", e);
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * Start file  based production
     * POST /api/kafka/producer/start-file
     * Body: { "file": "network.pcap", "topic": "network-flows", "maxPackets": 0 }
     */
    @PostMapping("/start-file")
    public ResponseEntity<Map<String, String>> startFileProduction(@RequestBody Map<String, Object> request) {
        try {
            String file = (String) request.get("file");
            String topic = (String) request.get("topic");
            int maxPackets = request.containsKey("maxPackets") 
                    ? ((Number) request.get("maxPackets")).intValue() 
                    : 0;

            if (file == null || topic == null) {
                throw new IllegalArgumentException("Missing required fields: file, topic");
            }

            producerService.startFileProduction(file, topic, maxPackets);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "File production started");
            response.put("file", file);
            response.put("topic", topic);

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            LOG.error("Failed to start file production", e);
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * Stop the currently running producer
     * POST /api/kafka/producer/stop
     */
    @PostMapping("/stop")
    public ResponseEntity<Map<String, String>> stopProducer() {
        try {
            producerService.stopProducer();

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Producer stopped");

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            LOG.error("Failed to stop producer", e);
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * List available network interfaces
     * GET /api/kafka/producer/interfaces
     */
    @GetMapping("/interfaces")
    public ResponseEntity<Map<String, Object>> listInterfaces() {
        List<Map<String, String>> interfaces = producerService.listNetworkInterfaces();
        
        Map<String, Object> response = new HashMap<>();
        response.put("interfaces", interfaces);
        response.put("count", interfaces.size());

        return ResponseEntity.ok(response);
    }

    /**
     * List available PCAP files
     * GET /api/kafka/producer/files
     */
    @GetMapping("/files")
    public ResponseEntity<Map<String, Object>> listFiles() {
        List<String> files = producerService.listPcapFiles();

        Map<String, Object> response = new HashMap<>();
        response.put("files", files);
        response.put("count", files.size());

        return ResponseEntity.ok(response);
    }
}
