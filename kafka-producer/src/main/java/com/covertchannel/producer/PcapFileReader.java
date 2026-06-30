package com.covertchannel.producer;

import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.Pcaps;
import org.pcap4j.packet.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Non-static, injectable PCAP file reader
 */
public class PcapFileReader {

    private static final Logger LOG = LoggerFactory.getLogger(PcapFileReader.class);
    private static final int DEFAULT_MAX_CONSECUTIVE_FAILURES = 100;

    private final String pcapFilePath;
    private final KafkaPacketProducer producer;
    private final int maxPackets;
    private final int maxConsecutiveFailures;

    /**
     * Constructor with default failure threshold
     */
    public PcapFileReader(String pcapFilePath, KafkaPacketProducer producer, int maxPackets) {
        this(pcapFilePath, producer, maxPackets, DEFAULT_MAX_CONSECUTIVE_FAILURES);
    }

    /**
     * Constructor with configurable failure threshold
     */
    public PcapFileReader(String pcapFilePath, KafkaPacketProducer producer, int maxPackets, int maxConsecutiveFailures) {
        this.pcapFilePath = pcapFilePath;
        this.producer = producer;
        this.maxPackets = maxPackets;
        this.maxConsecutiveFailures = maxConsecutiveFailures;
    }

    /**
     * Process the PCAP file and send packets to Kafka
     * @return Processing result with statistics
     * @throws Exception if too many consecutive failures occur or file cannot be read
     */
    public ProcessingResult process() throws Exception {
        LOG.info("Processing PCAP file: {} (maxPackets: {}, failureThreshold: {})",
                pcapFilePath, maxPackets, maxConsecutiveFailures);

        long packetsSent = 0;
        long packetsFailed = 0;
        long bytesRead = 0;
        int consecutiveFailures = 0;
        try (PcapHandle handle = Pcaps.openOffline(pcapFilePath)) {
            int dlt = handle.getDlt().value();
            LOG.info("PCAP DLT: {}", dlt);

            Packet packet;

            while ((packet = handle.getNextPacket()) != null) {
                try {
                    long timestamp = handle.getTimestamp().getTime(); //in milliseconds
                    producer.sendPacketAsync(packet, dlt, timestamp);
                    packetsSent++;
                    bytesRead += packet.length();
                    consecutiveFailures = 0;

                    if (maxPackets > 0 && packetsSent >= maxPackets) {
                        LOG.info("Reached max packets limit: {}", maxPackets);
                        break;
                    }

                } catch (Exception e) {
                    packetsFailed++;
                    consecutiveFailures++;
                    
                    if (consecutiveFailures >= maxConsecutiveFailures) {
                        LOG.error("Too many consecutive failures ({}), aborting. Last error: {}", 
                                  consecutiveFailures, e.getMessage());
                        throw new Exception("Processing aborted due to repeated failures", e);
                    }
                    
                    if (packetsFailed % 10 == 0) { // Log every 10th failure to avoid spam
                        LOG.warn("Failed to process packet (total failures: {}): {}", 
                                 packetsFailed, e.getMessage());
                    }
                }
            }

            LOG.info("Finished processing: {} packets sent, {} failed, {} bytes", 
                     packetsSent, packetsFailed, bytesRead);
        }

        return new ProcessingResult(packetsSent, packetsFailed, bytesRead);
    }

    /**
     * Processing result holder
     */
    public static class ProcessingResult {
        private final long packetsSent;
        private final long packetsFailed;
        private final long bytesRead;

        public ProcessingResult(long packetsSent, long packetsFailed, long bytesRead) {
            this.packetsSent = packetsSent;
            this.packetsFailed = packetsFailed;
            this.bytesRead = bytesRead;
        }

        public long getPacketsSent() { return packetsSent; }
        public long getPacketsFailed() { return packetsFailed; }
        public long getBytesRead() { return bytesRead; }
    }
}
