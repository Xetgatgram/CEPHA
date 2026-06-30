package com.covertchannel.algorithm.detector;

import com.covertchannel.framework.api.*;
import com.covertchannel.processor.DetectionResult;
import com.covertchannel.processor.PacketFeatures;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.IpV6Packet;
import org.pcap4j.packet.TcpPacket;
import org.pcap4j.packet.UdpPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Port-Entropy-based covert channel detector.
 *
 * <p>Detection Principle:</p>
 * Normal traffic between two hosts uses a small, stable set of destination ports.
 * Port-hopping covert channels cycle through many different destination ports
 * to encode data or evade detection, producing abnormally high Shannon entropy
 * over the destPort distribution within a time window.
 *
 * <ul>
 *   <li>Key:     srcIP -&gt; destIP  (all connections between a host pair in one window)</li>
 *   <li>Feature: destPort per packet (TCP or UDP)</li>
 *   <li>Score:   Shannon entropy H = -sum(p_i * log2(p_i))</li>
 * </ul>
 *
 * <p>config.json keys:</p>
 * <ul>
 *   <li>entropyThreshold  (double, default 3.0)  – anomaly if H &gt;= threshold</li>
 *   <li>minPackets        (int,    default 10)   – minimum packets before scoring</li>
 *   <li>executionMode     (String, default "")</li>
 *   <li>windowType        (String, default "")</li>
 *   <li>testlauf          (String, default "unknown")</li>
 * </ul>
 */
public class PortEntropyDetector implements DetectionAlgorithm {

    private static final long   serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PortEntropyDetector.class);

    // Config
    private double entropyThreshold = 3.0;
    private int    minPackets       = 10;
    private String testlauf         = "unknown";
    private String executionMode    = "";
    private String windowType       = "";

    // State
    private Map<Integer, Integer> portCounts; // destPort -> Häufigkeit
    private int    packetCount;
    private int    skippedCount;              // Pakete ohne erkannten TCP/UDP Port (z.B. ICMP)
    private String flowId;


    @Override
    public void initialize(FrameworkConfig config) {
        this.entropyThreshold = config.getDouble("entropyThreshold", 3.0);
        this.minPackets       = config.getInt("minPackets", 10);
        this.executionMode    = config.getString("executionMode", "");
        this.windowType       = config.getString("windowType", "");
        this.portCounts  = new HashMap<>();
        this.packetCount = 0;
        this.skippedCount = 0;
        this.flowId      = null;

        LOG.info("PortEntropyDetector initialized – entropyThreshold={}, minPackets={}",
                entropyThreshold, minPackets);
    }


    //  Feature Extraction default implementation is used


    @Override
    public boolean supportsFeatureExtraction() {
        return true;
    }

    /**
     * Extracts only the destPort from the raw packet
     * The raw packet object can be GC immediately after this call
     * Returns -1 in "dest_port" if the packet has no TCP/UDP layer e.g. ICMP
     */
    @Override
    public PacketFeatureExtractor getFeatureExtractor() {
        return packet -> {
            int destPort = -1;

            org.pcap4j.packet.Packet raw = packet.getRawPacket();
            if (raw != null) {
                TcpPacket tcp = raw.get(TcpPacket.class);
                if (tcp != null) {
                    destPort = tcp.getHeader().getDstPort().valueAsInt();
                } else {
                    UdpPacket udp = raw.get(UdpPacket.class);
                    if (udp != null) {
                        destPort = udp.getHeader().getDstPort().valueAsInt();
                    }
                }
            }

            return new PacketFeatures(packet.getCaptureTimestamp())
                    .add("dest_port", destPort);
        };
    }

    /**
     * Accumulates destPort counts per window
     * Called by the framework for each PacketFeatures in the window
     */
    @Override
    public void processFlowFeatures(PacketFeatures features) {
        if (features == null) return;

        if (flowId == null || flowId.isEmpty()) {
            flowId = features.getCustomKey();
        }

        packetCount++;

        int destPort = features.getInt("dest_port", -1);
        if (destPort < 0) {
            skippedCount++; // ICMP oder nicht-TCP/UDP
            return;
        }

        portCounts.merge(destPort, 1, Integer::sum);
    }

    // Detection

    /**
     * Computes Shannon entropy over the accumulated destPort distribution
     * Returns null if not enough packets were seen below minPackets
     */
    @Override
    public DetectionResult detect() throws Exception {
        if (packetCount < minPackets) {
            LOG.debug("flowId={} – not enough packets: {} < {}", flowId, packetCount, minPackets);
            return null;
        }

        int effectivePackets = packetCount - skippedCount;
        if (effectivePackets <= 0 || portCounts.isEmpty()) {
            LOG.debug("flowId={} – no TCP/UDP packets to evaluate", flowId);
            return null;
        }

        double  entropy     = calculateShannonEntropy(portCounts, effectivePackets);
        int     uniquePorts = portCounts.size();
        boolean isAnomaly   = entropy >= entropyThreshold;

        if (isAnomaly) {
            LOG.warn("Port-hopping DETECTED flowId={} – H={} (>= threshold={}), uniquePorts={}",
                    flowId, String.format("%.4f", entropy), entropyThreshold, uniquePorts);
        } else {
            LOG.debug("flowId={} – H={}, uniquePorts={}",
                    flowId, String.format("%.4f", entropy), uniquePorts);
        }

        return new DetectionResult()
                .addDetail("flowId",     flowId)
                .addDetail("entropy_score",     entropy)
                .addDetail("unique_ports",      uniquePorts)
                .addDetail("packet_count",      packetCount)
                .addDetail("skipped_count",     skippedCount)
                .addDetail("entropy_threshold", entropyThreshold)
                .addDetail("is_anomaly",        isAnomaly ? 1.0 : 0.0)
                .addDetail("executionMode",     executionMode)
                .addDetail("windowType",        windowType)
                .addDetail("testlauf",        windowType);
    }

    // Window Reset & Cleanup

    /**
     * Called by the framework after each window
     * Resets per-window state so the next window starts clean
     */
    @Override
    public void resetAfterWindow() {
        portCounts.clear();
        packetCount  = 0;
        skippedCount = 0;
        flowId       = null;
    }

    @Override
    public void close() {
        if (portCounts != null) portCounts.clear();
        packetCount  = 0;
        skippedCount = 0;
        flowId       = null;
        LOG.info("PortEntropyDetector closed");
    }


    // KeyExtractor: srcIP -> destIP
    // Ports werden  nicht in den Key aufgenommen:
    // Nur so akkumulieren wir alle Verbindungen eines Host-Paares
    // und können die destPort-Verteilung (Entropie) berechnen.
    // Default-Implementierung gibt uns dies bereits so zurück

    // Shannon Entropy


    /**
     * Calculates Shannon entropy: H = -sum(p_i * log2(p_i))
     *
     * Theoretical bounds:
     *   H = 0.0         -> all packets use the same port (no hopping)
     *   H = log2(n)     -> perfectly uniform distribution over n unique ports
     *
     * @param counts map of destPort to packet count
     * @param total  total number of TCP/UDP packets
     * @return entropy in bits
     */
    private double calculateShannonEntropy(Map<Integer, Integer> counts, int total) {
        double entropy = 0.0;
        for (int count : counts.values()) {
            double p = (double) count / total;
            if (p > 0.0) {
                entropy -= p * (Math.log(p) / Math.log(2));
            }
        }
        return entropy;
    }
}
