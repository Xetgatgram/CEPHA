
package com.covertchannel.framework.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class KafkaAdminService {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminService.class);

    private final KafkaProperties kafkaProperties;
    private AdminClient adminClient;

    public KafkaAdminService(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");

        adminClient = AdminClient.create(props);
        LOG.info("Kafka AdminClient initialized with brokers: {}", kafkaProperties.getBootstrapServers());

        if (kafkaProperties.getAdmin().isAutoCreateTopics()) {
            createDefaultTopics();
        }
    }

    @PreDestroy
    public void cleanup() {
        if (adminClient != null) {
            adminClient.close();
            LOG.info("Kafka AdminClient closed");
        }
    }

    /**
     * Erstellt die konfigurierten Standard-Topics beim Start
     */
    private void createDefaultTopics() {
        List<KafkaProperties.TopicConfig> defaultTopics = kafkaProperties.getAdmin().getDefaultTopics();

        if (defaultTopics.isEmpty()) {
            LOG.info("No default topics configured");
            return;
        }

        try {
            Set<String> existingTopics = listTopics();
            List<NewTopic> topicsToCreate = new ArrayList<>();

            for (KafkaProperties.TopicConfig topicConfig : defaultTopics) {
                if (!existingTopics.contains(topicConfig.getName())) {
                    NewTopic newTopic = new NewTopic(
                            topicConfig.getName(),
                            topicConfig.getPartitions(),
                            topicConfig.getReplicationFactor()
                    );

                    // Retention konfigurieren
                    Map<String, String> configs = new HashMap<>();
                    configs.put("retention.ms", String.valueOf(topicConfig.getRetentionMs()));
                    newTopic.configs(configs);

                    topicsToCreate.add(newTopic);
                    LOG.info("Scheduling topic creation: {}", topicConfig.getName());
                }
            }

            if (!topicsToCreate.isEmpty()) {
                CreateTopicsResult result = adminClient.createTopics(topicsToCreate);
                result.all().get();
                LOG.info("Successfully created {} default topics", topicsToCreate.size());
            } else {
                LOG.info("All default topics already exist");
            }

        } catch (Exception e) {
            LOG.warn("Failed to create default topics: {}", e.getMessage());
        }
    }

    /**
     * Listet alle Topics im Cluster
     */
    public Set<String> listTopics() throws ExecutionException, InterruptedException {
        ListTopicsResult result = adminClient.listTopics();
        return result.names().get();
    }

    /**
     * Gibt detaillierte Informationen zu einem Topic zurück
     */
    public TopicInfo getTopicInfo(String topicName) throws ExecutionException, InterruptedException {
        // 1 Topic Metadaten abrufen
        DescribeTopicsResult describeResult = adminClient.describeTopics(Collections.singleton(topicName));
        TopicDescription description = describeResult.allTopicNames().get().get(topicName);

        // 2 Retention Config abrufen
        DescribeConfigsResult configResult = adminClient.describeConfigs(
                Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC, topicName))
        );
        Config config = configResult.all().get().get(new ConfigResource(ConfigResource.Type.TOPIC, topicName));

        // 3 Message Anzahl berechnen Summe aller Partitionen
        long totalMessages = calculateTotalMessages(topicName, description.partitions().size());
        
        // 4 Partition Details abrufen für detaillierte Ansicht
        List<PartitionInfo> partitionDetails = getPartitionDetails(topicName, description);

        return new TopicInfo(
                description.name(),
                description.partitions().size(),
                description.partitions().get(0).replicas().size(),
                config.get("retention.ms") != null ? config.get("retention.ms").value() : "unknown",
                totalMessages,
                partitionDetails
        );
    }

    /**
     * Berechnet die Gesamtanzahl der Messages in einem Topic
     * Summe über alle Partitionen endOffset - beginOffset
     */
    private long calculateTotalMessages(String topicName, int numPartitions) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaProperties.getBootstrapServers());
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("group.id", "admin-temp-" + UUID.randomUUID());
        props.put("enable.auto.commit", "false");

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = new ArrayList<>();
            for (int i = 0; i < numPartitions; i++) {
                partitions.add(new TopicPartition(topicName, i));
            }

            // Hole Begin- und End-Offsets
            Map<TopicPartition, Long> beginOffsets = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            long total = 0;
            for (TopicPartition partition : partitions) {
                long begin = beginOffsets.getOrDefault(partition, 0L);
                long end = endOffsets.getOrDefault(partition, 0L);
                total += (end - begin);
            }

            LOG.debug("Calculated total messages for topic {}: {}", topicName, total);
            return total;

        } catch (Exception e) {
            LOG.error("Failed to calculate message count for topic {}: {}", topicName, e.getMessage());
            return 0;
        }
    }

    /**
     * Holt detaillierte Partition-Informationen
     */
    private List<PartitionInfo> getPartitionDetails(String topicName, TopicDescription description) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaProperties.getBootstrapServers());
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("group.id", "admin-temp-" + UUID.randomUUID());
        props.put("enable.auto.commit", "false");

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = new ArrayList<>();

            for (org.apache.kafka.common.TopicPartitionInfo partition : description.partitions()) {
                TopicPartition tp = new TopicPartition(topicName, partition.partition());
                
                long beginOffset = consumer.beginningOffsets(Collections.singletonList(tp)).get(tp);
                long endOffset = consumer.endOffsets(Collections.singletonList(tp)).get(tp);
                long messages = endOffset - beginOffset;

                partitionInfos.add(new PartitionInfo(
                        partition.partition(),
                        partition.leader() != null ? partition.leader().id() : -1,
                        messages,
                        beginOffset,
                        endOffset
                ));
            }

            return partitionInfos;

        } catch (Exception e) {
            LOG.error("Failed to get partition details for topic {}: {}", topicName, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Erstellt ein neues Topic
     */
    public void createTopic(String topicName, int partitions, short replicationFactor, Long retentionMs)
            throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

        if (retentionMs != null) {
            Map<String, String> configs = new HashMap<>();
            configs.put("retention.ms", String.valueOf(retentionMs));
            newTopic.configs(configs);
        }

        CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
        result.all().get();
        LOG.info("Created topic: {}", topicName);
    }

    /**
     * Löscht ein Topic
     */
    public void deleteTopic(String topicName) throws ExecutionException, InterruptedException {
        DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(topicName));
        result.all().get();
        LOG.info("Deleted topic: {}", topicName);
    }

    /**
     * Listet alle Consumer-Gruppen
     */
    public Set<String> listConsumerGroups() throws ExecutionException, InterruptedException {
        ListConsumerGroupsResult result = adminClient.listConsumerGroups();
        return result.all().get().stream()
                .map(ConsumerGroupListing::groupId)
                .collect(Collectors.toSet());
    }

    /**
     * Gibt Informationen zu einer Consumer-Gruppe zurück
     */
    public ConsumerGroupInfo getConsumerGroupInfo(String groupId) throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(Collections.singleton(groupId));
        ConsumerGroupDescription description = describeResult.all().get().get(groupId);

        ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(groupId);
        Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets =
                offsetsResult.partitionsToOffsetAndMetadata().get();

        return new ConsumerGroupInfo(
                description.groupId(),
                description.state().toString(),
                description.members().size(),
                offsets.size()
        );
    }

    public void deleteConsumerGroup(String groupId) throws ExecutionException, InterruptedException {
        DeleteConsumerGroupsResult result = adminClient.deleteConsumerGroups(Collections.singleton(groupId));
        result.all().get();
        LOG.info("Deleted consumer group: {}", groupId);
    }

    /**
     * Prüft Cluster Verbindung
     */
    public boolean checkConnection() {
        try {
            adminClient.describeCluster().clusterId().get();
            return true;
        } catch (Exception e) {
            LOG.error("Kafka connection check failed: {}", e.getMessage());
            return false;
        }
    }

    // DTO-Klassen
    public static class TopicInfo {
        private final String name;
        private final int partitions;
        private final int replicationFactor;
        private final String retentionMs;
        private final long totalMessages;
        private final List<PartitionInfo> partitionDetails;

        public TopicInfo(String name, int partitions, int replicationFactor, String retentionMs, 
                         long totalMessages, List<PartitionInfo> partitionDetails) {
            this.name = name;
            this.partitions = partitions;
            this.replicationFactor = replicationFactor;
            this.retentionMs = retentionMs;
            this.totalMessages = totalMessages;
            this.partitionDetails = partitionDetails;
        }

        public String getName() { return name; }
        public int getPartitions() { return partitions; }
        public int getReplicationFactor() { return replicationFactor; }
        public String getRetentionMs() { return retentionMs; }
        public long getTotalMessages() { return totalMessages; }
        public List<PartitionInfo> getPartitionDetails() { return partitionDetails; }
    }

    public static class PartitionInfo {
        private final int partition;
        private final int leader;
        private final long messages;
        private final long beginOffset;
        private final long endOffset;

        public PartitionInfo(int partition, int leader, long messages, long beginOffset, long endOffset) {
            this.partition = partition;
            this.leader = leader;
            this.messages = messages;
            this.beginOffset = beginOffset;
            this.endOffset = endOffset;
        }

        public int getPartition() { return partition; }
        public int getLeader() { return leader; }
        public long getMessages() { return messages; }
        public long getBeginOffset() { return beginOffset; }
        public long getEndOffset() { return endOffset; }
    }

    public static class ConsumerGroupInfo {
        private final String groupId;
        private final String state;
        private final int members;
        private final int assignedPartitions;

        public ConsumerGroupInfo(String groupId, String state, int members, int assignedPartitions) {
            this.groupId = groupId;
            this.state = state;
            this.members = members;
            this.assignedPartitions = assignedPartitions;
        }

        public String getGroupId() { return groupId; }
        public String getState() { return state; }
        public int getMembers() { return members; }
        public int getAssignedPartitions() { return assignedPartitions; }
    }
}