package com.covertchannel.framework.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api/kafka")
public class KafkaAdminRestController {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminRestController.class);
    private final KafkaAdminService kafkaAdminService;

    public KafkaAdminRestController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    /**
     * Gesundheitscheck für Kafka-Verbindung
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> response = new HashMap<>();
        boolean connected = kafkaAdminService.checkConnection();
        
        response.put("status", connected ? "UP" : "DOWN");
        response.put("kafka", connected);
        
        return ResponseEntity.ok(response);
    }

    /**
     * Listet alle Topics
     */
    @GetMapping("/topics")
    public ResponseEntity<Map<String, Object>> listTopics() {
        try {
            Set<String> topics = kafkaAdminService.listTopics();
            Map<String, Object> response = new HashMap<>();
            response.put("topics", topics);
            response.put("count", topics.size());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            LOG.error("Failed to list topics", e);
            return ResponseEntity.internalServerError()
                .body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Gibt Details zu einem Topic zurück
     */
    @GetMapping("/topics/{topicName}")
    public ResponseEntity<Object> getTopicInfo(@PathVariable String topicName) {
        try {
            KafkaAdminService.TopicInfo info = kafkaAdminService.getTopicInfo(topicName);
            return ResponseEntity.ok(info);
        } catch (Exception e) {
            LOG.error("Failed to get topic info for: {}", topicName, e);
            return ResponseEntity.internalServerError()
                .body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Erstellt ein neues Topic
     */
    @PostMapping("/topics")
    public ResponseEntity<Map<String, String>> createTopic(@RequestBody CreateTopicRequest request) {
        try {
            kafkaAdminService.createTopic(
                request.getName(),
                request.getPartitions(),
                request.getReplicationFactor(),
                request.getRetentionMs()
            );
            return ResponseEntity.ok(Map.of("message", "Topic created successfully", "topic", request.getName()));
        } catch (Exception e) {
            LOG.error("Failed to create topic: {}", request.getName(), e);
            return ResponseEntity.internalServerError()
                .body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Löscht ein Topic
     */
    @DeleteMapping("/topics/{topicName}")
    public ResponseEntity<Map<String, String>> deleteTopic(@PathVariable String topicName) {
        try {
            kafkaAdminService.deleteTopic(topicName);
            return ResponseEntity.ok(Map.of("message", "Topic deleted successfully", "topic", topicName));
        } catch (Exception e) {
            LOG.error("Failed to delete topic: {}", topicName, e);
            return ResponseEntity.internalServerError()
                .body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Listet alle Consumer
     */
    @GetMapping("/consumer-groups")
    public ResponseEntity<Map<String, Object>> listConsumerGroups() {
        try {
            Set<String> groups = kafkaAdminService.listConsumerGroups();
            Map<String, Object> response = new HashMap<>();
            response.put("groups", groups);
            response.put("count", groups.size());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            LOG.error("Failed to list consumer groups", e);
            return ResponseEntity.internalServerError()
                .body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Gibt Details zu  Consumer Gruppe zurück
     */
    @GetMapping("/consumer-groups/{groupId}")
    public ResponseEntity<Object> getConsumerGroupInfo(@PathVariable String groupId) {
        try {
            KafkaAdminService.ConsumerGroupInfo info = kafkaAdminService.getConsumerGroupInfo(groupId);
            return ResponseEntity.ok(info);
        } catch (Exception e) {
            LOG.error("Failed to get consumer group info for: {}", groupId, e);
            return ResponseEntity.internalServerError()
                .body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Löscht eine Consumer Gruppe muss EMPTY/inaktiv sein
     */
    @DeleteMapping("/consumer-groups/{groupId}")
    public ResponseEntity<Map<String, String>> deleteConsumerGroup(@PathVariable String groupId) {
        try {
            kafkaAdminService.deleteConsumerGroup(groupId);
            return ResponseEntity.ok(Map.of(
                    "message", "Consumer Group gelöscht",
                    "groupId", groupId
            ));
        } catch (Exception e) {
            LOG.error("Failed to delete consumer group: {}", groupId, e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", e.getMessage()));
        }
    }

    // Request DTOs
    public static class CreateTopicRequest {
        private String name;
        private int partitions = 1;
        private short replicationFactor = 1;
        private Long retentionMs;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public int getPartitions() { return partitions; }
        public void setPartitions(int partitions) { this.partitions = partitions; }
        public short getReplicationFactor() { return replicationFactor; }
        public void setReplicationFactor(short replicationFactor) { this.replicationFactor = replicationFactor; }
        public Long getRetentionMs() { return retentionMs; }
        public void setRetentionMs(Long retentionMs) { this.retentionMs = retentionMs; }
    }
}
