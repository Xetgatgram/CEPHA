// framework-rest-api/src/main/java/com/covertchannel/framework/kafka/KafkaProperties.java

package com.covertchannel.framework.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {


    private String bootstrapServers = "localhost:29092"; //lokaler Fallback

    private Admin admin = new Admin();
    private Producer producer = new Producer();

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Admin getAdmin() {
        return admin;
    }

    public void setAdmin(Admin admin) {
        this.admin = admin;
    }

    public Producer getProducer() {
        return producer;
    }

    public void setProducer(Producer producer) {
        this.producer = producer;
    }

    public static class Admin {
        private boolean autoCreateTopics = true;
        private List<TopicConfig> defaultTopics = new ArrayList<>();

        public boolean isAutoCreateTopics() {
            return autoCreateTopics;
        }

        public void setAutoCreateTopics(boolean autoCreateTopics) {
            this.autoCreateTopics = autoCreateTopics;
        }

        public List<TopicConfig> getDefaultTopics() {
            return defaultTopics;
        }

        public void setDefaultTopics(List<TopicConfig> defaultTopics) {
            this.defaultTopics = defaultTopics;
        }
    }

    public static class TopicConfig {
        private String name;
        private int partitions = 1;
        private short replicationFactor = 1;
        private long retentionMs = 604800000L; // 7 days

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getPartitions() {
            return partitions;
        }

        public void setPartitions(int partitions) {
            this.partitions = partitions;
        }

        public short getReplicationFactor() {
            return replicationFactor;
        }

        public void setReplicationFactor(short replicationFactor) {
            this.replicationFactor = replicationFactor;
        }

        public long getRetentionMs() {
            return retentionMs;
        }

        public void setRetentionMs(long retentionMs) {
            this.retentionMs = retentionMs;
        }
    }

    public static class Producer {
        private String pcapUploadDir = "/tmp/pcap-uploads";
        private long maxFileSize = 1073741824L; // 1GB

        public String getPcapUploadDir() {
            return pcapUploadDir;
        }

        public void setPcapUploadDir(String pcapUploadDir) {
            this.pcapUploadDir = pcapUploadDir;
        }

        public long getMaxFileSize() {
            return maxFileSize;
        }

        public void setMaxFileSize(long maxFileSize) {
            this.maxFileSize = maxFileSize;
        }
    }
}
