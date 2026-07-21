package com.covertchannel.framework.api;

import com.covertchannel.processor.DetectionResult;
import com.covertchannel.processor.NetworkPacket;
import com.covertchannel.processor.PacketFeatures;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.IpV6Packet;

import java.io.Serial;
import java.io.Serializable;

/**
 * Base interface for all covert channel detection algorithms.
 *
 */
public interface DetectionAlgorithm extends Serializable {

    @Serial
    static final long serialVersionUID = 1L;
    /**
     * Initialize the algorithm with framework configuration.
     * Called once when the algorithm starts.
     *
     * @param config Framework configuration containing algorithm-specific parameters
     */
    void initialize(FrameworkConfig config);

    /**
     * Last call at Job-End, use to call cleanup, calculate last metrics.
     * Called once when the algorithm stops.
     */
    void close();



    /**
     * Process a single network flow.
     * Called for each flow in the stream.
     * Must be implemented for Per-Packet processing (WindowType.NONE)
     * Can be implemented for Windowed processing
     * @param packet  The NetworkPacket flow to analyze
     *
     */
    default void processFlow(NetworkPacket packet) {};


    /**
     * Process PacketFeatures, extracted with PacketFeatureExtractor.
     * Must be implemented for processing FeatureExtraction Objects
     * @param features
     */
    default void processFlowFeatures(PacketFeatures features) {
        throw new UnsupportedOperationException(
                "Override processFlowFeatures() when supportsIncrementalAggregation() returns true."
        );
    }

    //------------------------------------------------


    /**
     * Generate detection result based on the processed Data
     * is called from the Framework
     * Example: dr = new DetectionResult();
     * dr.addDetail("score", variable_A);
     * dr.addDetail("anomaly", variable_B);
     * @return Detection result
     */
    DetectionResult detect() throws Exception;


    /**
     * Called by the framework after each window is processed.
     * Override to reset internal per-window state.
     *
     * Default: no-op — allows state accumulation across windows.
     * Override with state-clearing logic for window-isolated algorithms.
     */
    default void resetAfterWindow() {
        // no-op
    }


    /**
     * Indicates whether the detection algorithm supports FeatureExtraction.
     * If true, the Framework will call processFlowFeatures() instead of processFlow().
     * and The Framework will call getFeatureExtractor() to extract features.
     *
     * @return True if FeatureExtraction is supported, false otherwise
     */
    default boolean supportsFeatureExtraction() {
        return false; // Safe default: use complete NetworkPacket
    }


    //---------------------------------------------------------
    /**
     * Returns a extractor for FeatureExtraction
     * Called once per TaskManager slot the returned extractor is reused for all packets.
     *
     * Override this to extract only the features your algorithm needs,
     * so the raw packet payload can be garbage collected immediately.
     *
     * Default implementation returns a PacketFeatures object with the capture timestamp.
     *
     * @return PacketFeatureExtractor for this algorithm
     */
    default PacketFeatureExtractor getFeatureExtractor() {
        return packet -> new PacketFeatures(packet.getCaptureTimestamp()); // safe default
    }

    /**
     * Returns a lightweight extractor for partitioning.
     * Default implementation provides a standard src->dst key.
     */
    default PacketKeyExtractor getKeyExtractor() {
        return (netPacket) -> {
            if (netPacket == null) return "null";

            // access DLT if needed, or just access the raw packet
            org.pcap4j.packet.Packet packet = netPacket.getRawPacket();
            if (packet == null) {
                // Packet parsing failed - likely pcap4j dependency issue
                return "null";
            }

            // 1. Try IPv4
            IpV4Packet ip4 = packet.get(IpV4Packet.class);
            if (ip4 == null && packet.getPayload() instanceof IpV4Packet) {
                ip4 = (IpV4Packet) packet.getPayload();
            }
            if (ip4 != null) {
                return ip4.getHeader().getSrcAddr().getHostAddress() + "->" +
                        ip4.getHeader().getDstAddr().getHostAddress();
            }

            // 2. Try IPv6
            IpV6Packet ip6 = packet.get(IpV6Packet.class);
            if (ip6 == null && packet.getPayload() instanceof IpV6Packet) {
                ip6 = (IpV6Packet) packet.getPayload();
            }
            if (ip6 != null) {
                return ip6.getHeader().getSrcAddr().getHostAddress() + "->" +
                        ip6.getHeader().getDstAddr().getHostAddress();
            }

            // No IP layer found - might be ARP, raw Ethernet, or other non-IP traffic
            return "Unknown";
        };
    }

}