package com.covertchannel.producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.IpV6Packet;
import org.pcap4j.packet.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Produces network packets to a Kafka topic.
 * 
 * Serializes NetworkPacket objects to byte[] and sends them to Kafka.
 * Used to bridge between Pcap4j and Flink processing.
 */
public class KafkaPacketProducer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaPacketProducer.class);

    private final KafkaProducer<String, byte[]> producer;
    private final String topic;
    private long recordsSent;
    private long recordsFailed;

    /**
     * Initialize Kafka producer with default configuration
     *
     * @param brokers Kafka broker addresses (e.g. "localhost:9092")
     * @param topic   Target Kafka topic
     */
    public KafkaPacketProducer(String brokers, String topic) {
        this.topic = topic;
        this.recordsSent = 0;
        this.recordsFailed = 0;

        Properties props = createProducerConfig(brokers);
        this.producer = new KafkaProducer<String, byte[]>(props);

        LOG.info("KafkaPacketProducer initialized: brokers={}, topic={}", brokers, topic);
    }

    /**
     * Send packet asynchronously
     *
     * @param packet   The network packet to send
     * @param dlt      Data Link Type value
     * @param timestamp Capture timestamp in milliseconds
     */
    public void sendPacketAsync(Packet packet, int dlt, long timestamp) {
        try {
            byte[] value =  packet.getRawData();


            //  Buffer size must include Timestamp (8 bytes) + DLT (4 bytes)
            ByteBuffer buffer = ByteBuffer.allocate(12 + value.length);
            buffer.putInt(dlt);
            buffer.putLong(timestamp);
            buffer.put(value);
            //Key is for kafka to guarantee ordering over multiple partitions for that key.
            String key = extractSrcIp(packet);
            ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, key, buffer.array());

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    recordsFailed++;
                    LOG.error("Async send failed: {}", exception.getMessage(), exception);
                } else {
                    recordsSent++;
                    LOG.debug("Async send successful: offset={}", metadata.offset());
                }
            });

        } catch (Exception e) {
            recordsFailed++;
            LOG.error("Failed to queue packet: {}", e.getMessage(), e);
        }
    }

    private String extractSrcIp(Packet packet) {
        IpV4Packet ipV4 = packet.get(IpV4Packet.class);
        if (ipV4 != null) {
            return ipV4.getHeader().getSrcAddr().getHostAddress();
        }
        IpV6Packet ipV6 = packet.get(IpV6Packet.class);
        if (ipV6 != null) {
            return ipV6.getHeader().getSrcAddr().getHostAddress();
        }
        return null; // Non-IP ARP etc. Round-Robin, is ok
    }



    /**
     * Get producer statistics
     */
    public Statistics getStatistics() {
        return new Statistics(recordsSent, recordsFailed);
    }

    /**
     * Close the producer
     */
    public void close() {
        LOG.info("Closing KafkaPacketProducer. Sent: {}, Failed: {}", recordsSent, recordsFailed);
        producer.close();
    }

    /**
     * Create Kafka producer configuration
     */
    private Properties createProducerConfig(String brokers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // Reliability settings
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 2);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);

        // Compression
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        return props;
    }

    /**
     * Statistics holder
     */
    public static class Statistics {
        public final long recordsSent;
        public final long recordsFailed;
        public final double successRate;

        public Statistics(long recordsSent, long recordsFailed) {
            this.recordsSent = recordsSent;
            this.recordsFailed = recordsFailed;
            long total = recordsSent + recordsFailed;
            this.successRate = total > 0 ? (double) recordsSent / total * 100 : 0;
        }

        @Override
        public String toString() {
            return String.format("KafkaStats[sent=%d, failed=%d, success=%.1f%%]",
                    recordsSent, recordsFailed, successRate);
        }
    }
}
