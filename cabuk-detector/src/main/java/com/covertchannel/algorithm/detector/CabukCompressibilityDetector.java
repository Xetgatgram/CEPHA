package com.covertchannel.algorithm.detector;

import com.covertchannel.framework.api.DetectionAlgorithm;
import com.covertchannel.framework.api.FrameworkConfig;
import com.covertchannel.framework.api.PacketKeyExtractor;
import com.covertchannel.processor.DetectionResult;
import com.covertchannel.processor.NetworkPacket;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.IpV6Packet;
import org.pcap4j.packet.TcpPacket;
import org.pcap4j.packet.UdpPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Cabuk compressibility-based covert-channel detector (buffered).
 *
 * config.json keys:
 *   iatThresholdMs   (double, default 1000.0)
 *   precision        (int,    default 2)
 *   anomalyThreshold (double, default 0.7)
 *   minIats          (int,    default 5)
 *   windowCount      (int,    default 128)
 */
public class CabukCompressibilityDetector implements DetectionAlgorithm {

    private static final long   serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CabukCompressibilityDetector.class);

    // Index 0 → 'X' (no leading zeros), 1 → 'A', 2 → 'B', ...
    private static final char[] ZERO_PREFIX = "XABCDEFGHIJKLMNOPQRSTUVWYZ".toCharArray();

    private double iatThresholdMs  = 1_000.0;
    private int precision = 2;
    private double anomalyThreshold = 0.7;
    private int minIats = 5;

    private List<Long> timestamps;
    private int packetCount;
    private String flowId;
    private int flowChanges;
    private String executionMode = "";
    private String windowType = "";
    private String testlauf = "";

    // -------------------------------------------------------------------------

    @Override
    public void initialize(FrameworkConfig config) {
        this.iatThresholdMs = config.getDouble("iatThresholdMs",1_000.0);
        this.precision = config.getInt ("precision",2);
        this.anomalyThreshold = config.getDouble("anomalyThreshold",0.7);
        this.minIats = config.getInt ("minIats",5);
        this.timestamps = new ArrayList<>(config.getInt("windowCount",128));
        this.packetCount = 0;
        this.flowId = null;
        this.flowChanges = 0;
        this.testlauf = config.getString("testlauf", "unknown");
        this.executionMode = config.getString("executionMode", "");
        this.windowType = config.getString("windowType", "");
    }

    @Override
    public void processFlow(NetworkPacket packet) {
        packetCount++;
        if (packet == null) return;

        if (flowId == null || flowId.isEmpty()) {
            flowId = packet.getCustomKey();
        } else {
            if (!flowId.equals(packet.getCustomKey())) {
                flowChanges++;
            }
        }
        timestamps.add(packet.getCaptureTimestamp());
    }

    @Override
    public DetectionResult detect() throws Exception {
        if (timestamps.size() < 2) return null;

        Collections.sort(timestamps);

        List<Double> iats = new ArrayList<>(timestamps.size() - 1);
        double thresholdSeconds = iatThresholdMs / 1_000.0;
        for (int i = 0; i < timestamps.size() - 1; i++) {
            double iat = (timestamps.get(i + 1) - timestamps.get(i)) / 1_000.0;
            if (iat > 0 && iat <= thresholdSeconds) iats.add(iat);
        }

        if (iats.size() < minIats) return null;

        String encoded = encodeIats(iats);
        int originalLength = encoded.getBytes(StandardCharsets.UTF_8).length;
        int compressedLength = gzipLength(encoded);

        if (compressedLength <= 0) return null;

        double score = (double) originalLength / compressedLength;
        boolean anomaly = score > anomalyThreshold;

        DetectionResult result = new DetectionResult();
        result.addDetail("compressibility_score",score);
        result.addDetail("flowIdChanges",flowChanges);
        result.addDetail("executionMode",executionMode);
        result.addDetail("iat_count",timestamps.size());
        result.addDetail("windowType",windowType);
        result.addDetail("packet_count",packetCount);
        result.addDetail("precision",precision);
        result.addDetail("iat_threshold_ms", iatThresholdMs);
        result.addDetail("testlauf", testlauf);
        return result;
    }

    @Override
    public void close() {
        timestamps.clear();
        flowId = null;
        flowChanges = 0;
        packetCount = 0;
    }

    @Override
    public boolean supportsFeatureExtraction() {
        return false;
    }

    private String encodeIats(List<Double> iats) {
        StringBuilder sb = new StringBuilder(iats.size() * (precision + 1));
        for (double iat : iats)
            try {
                sb.append(encodeIat(iat));
            }catch (ArrayIndexOutOfBoundsException e) {
                LOG.debug("flowId={} IAT skipped: {}ms", flowId, iat);
            }
        return sb.toString();
    }



    String encodeIat(double iatSec) {
        return encodeIatInternal(iatSec, precision);
    }

    private String encodeIatInternal(double iatSec, int precision) {
        if (iatSec <= 0.0) {
            return "Z";
        }

        double x = iatSec;
        int zeroCount = 0;

        while (x < 0.1) {
            x *= 10.0;
            zeroCount++;
            if (zeroCount >= ZERO_PREFIX.length) {
                return "Z";
            }
        }

        int scale = 1;
        for (int i = 1; i < precision + 1; i++) {
            scale *= 10;
        }

        int rounded = (int) Math.round(x * scale);

        if (rounded >= scale) {
            rounded /= 10;
            zeroCount--;
        }

        if(zeroCount <= 0)
        {
            return String.valueOf(rounded);
        }
        else { // > 0
            zeroCount--;
        }

        return String.valueOf(ZERO_PREFIX[zeroCount]) + rounded;
    }


    private int gzipLength(String input) {
        byte[] raw = input.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(raw.length);
        try {
            GZIPOutputStream gz = new GZIPOutputStream(baos);
            gz.write(raw);
            gz.finish();
            gz.close();
        } catch (IOException e) {
            LOG.error("GZIP failed: {}", e.getMessage(), e);
            return -1;
        }
        return baos.toByteArray().length;
    }

    @Override
    public void resetAfterWindow()
    {
        timestamps.clear();
        flowId = null;
        flowChanges = 0;
        packetCount = 0;
    }

    @Override
    public PacketKeyExtractor getKeyExtractor() {
        return netPacket -> {
            if (netPacket == null) return "unknown";
            org.pcap4j.packet.Packet raw = netPacket.getRawPacket();
            if (raw == null) return "unknown";

            String srcIp = null, dstIp = null, proto = "unknown";
            int srcPort = 0, dstPort = 0;

            IpV4Packet ip4 = raw.get(IpV4Packet.class);
            if (ip4 != null) {
                srcIp = ip4.getHeader().getSrcAddr().getHostAddress();
                dstIp = ip4.getHeader().getDstAddr().getHostAddress();
                proto = ip4.getHeader().getProtocol().name();
            }

            if (srcIp == null) {
                IpV6Packet ip6 = raw.get(IpV6Packet.class);
                if (ip6 != null) {
                    srcIp = ip6.getHeader().getSrcAddr().getHostAddress();
                    dstIp = ip6.getHeader().getDstAddr().getHostAddress();
                    proto = ip6.getHeader().getNextHeader().name();
                }
            }

            if (srcIp == null) return "unknown";

            TcpPacket tcp = raw.get(TcpPacket.class);
            if (tcp != null) {
                srcPort = tcp.getHeader().getSrcPort().valueAsInt();
                dstPort = tcp.getHeader().getDstPort().valueAsInt();
            } else {
                UdpPacket udp = raw.get(UdpPacket.class);
                if (udp != null) {
                    srcPort = udp.getHeader().getSrcPort().valueAsInt();
                    dstPort = udp.getHeader().getDstPort().valueAsInt();
                }
            }

            return srcIp + ":" + srcPort + "->" + dstIp + ":" + dstPort + "|" + proto;
        };
    }
}