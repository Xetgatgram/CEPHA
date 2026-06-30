package com.covertchannel.algorithm.detector;

import com.covertchannel.framework.api.*;
import com.covertchannel.processor.DetectionResult;
import com.covertchannel.processor.NetworkPacket;
import com.covertchannel.processor.PacketFeatures;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.IpV6Packet;
import org.pcap4j.packet.TcpPacket;
import org.pcap4j.packet.UdpPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Cabuk compressibility-based covert-channel detector (Feature).
 *
 * config.json keys:
 *   iatThresholdMs   (double, default 1000.0)
 *   precision        (int,    default 2)
 *   anomalyThreshold (double, default 0.7)
 *   minIats          (int,    default 5)
 *   windowCount      (int,    default 128)
 */
public class CabukCompressibilityDetectorIncremental  implements DetectionAlgorithm {

    private static final long   serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CabukCompressibilityDetectorIncremental.class);

    // Index 0 → 'X' (no leading zeros), 1 → 'A', 2 → 'B', ...
    private static final char[] ZERO_PREFIX = "XABCDEFGHIJKLMNOPQRSTUVWYZ".toCharArray();

    private double iatThresholdMs  = 1_000.0;
    private int precision = 2;
    private double anomalyThreshold = 0.7;
    private int minIats = 5;

    private long lastTimestamp = -1L;   // -1 = noch kein Paket gesehen
    private StringBuilder encoded;               // akkumulierter Encoding-String
    private int iatCount;              // Anzahl erfolgreich encodierter IATs
    private int packetCount;
    private String flowId;
    private int flowChanges = 0;
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
        int windowCount = config.getInt("windowCount", 128);
        this.encoded = new StringBuilder(windowCount * (precision + 1));
        this.packetCount = 0;
        flowId = null;
        flowChanges = 0;
        this.executionMode = config.getString("executionMode", "");
        this.windowType = config.getString("windowType", "");
        this.testlauf = config.getString("testlauf", "unknown");
    }

    @Override
    /**
     * Verarbeitet ein eingehendes Paket sofort:
     *   1. IAT zum vorherigen Paket berechnen
     *   2. Filtern (out-of-order, Idle-Gaps)
     *   3. Direkt encodieren und an encoded anhängen
     */
    public void processFlowFeatures(PacketFeatures features) {
            if (features == null) return;

            if (flowId==null || flowId.isEmpty()) {
                flowId = features.getCustomKey();
            }else{
                if (!flowId.equals(features.getCustomKey())) {
                    flowChanges++;
                }
            }

            long ts = features.getCaptureTimestamp();
            if (ts < 0) return;

            // Erstes Paket ,nur Timestamp merken, noch keine IAT berechenbar
            if (lastTimestamp < 0) {
                lastTimestamp = ts;
                return;
            }

            double iat = (ts - lastTimestamp) / 1_000.0;  // ms → Sekunden
            lastTimestamp = ts;

            // Out-of-order (Flink/Watermark sollte das verhindern, defensiv abgesichert)
            if (iat <= 0) {
                LOG.debug("flowId={} – out-of-order packet skipped (iat={}s)", flowId, iat);
                return;
            }

            // Idle-Gap herausfiltern
            double thresholdSeconds = iatThresholdMs / 1_000.0;
            if (iat > thresholdSeconds) {
                LOG.debug("flowId={} – idle gap skipped (iat={}s > threshold={}s)",
                        flowId, iat, thresholdSeconds);
                return;
            }

            //  codieren
            try {
                encoded.append(encodeIat(iat));
                iatCount++;
            } catch (ArrayIndexOutOfBoundsException e) {
                LOG.debug("flowId={} – IAT skipped (too many leading zeros): {}s", flowId, iat);
            }

    }


    /**
     * Berechnet den Compressibility-Score auf dem akkumulierten Encoding-String.
     * Encoding in processFlow() erledigt.
     */
    @Override
    public DetectionResult detect() throws Exception {
        if (iatCount < minIats) {
            LOG.debug("flowId={} – not enough IATs: {} < {}", flowId, iatCount, minIats);
            return null;
        }
        String encodedStr = encoded.toString();
        int originalLength = encodedStr.getBytes(StandardCharsets.UTF_8).length;
        int compressedLength = gzipLength(encodedStr);

        if (compressedLength <= 0) return null;

        double  score = (double) originalLength / compressedLength;
        boolean anomaly = score > anomalyThreshold;


        DetectionResult result = new DetectionResult();
        result.addDetail("compressibility_score",score);
        result.addDetail("executionMode",executionMode);
        result.addDetail("flowIdChanges",flowChanges);
        result.addDetail("windowType",windowType);
        result.addDetail("packet_count",packetCount);
        result.addDetail("iat_count",iatCount);
        result.addDetail("precision",precision);
        result.addDetail("iat_threshold_ms", iatThresholdMs);
        result.addDetail("testlauf", testlauf);
        return result;
    }

    @Override
    public void close() {
        if (encoded != null) encoded.setLength(0);
        lastTimestamp = -1L;
        flowId = null;
        flowChanges = 0;
        iatCount = 0;
        packetCount = 0;
    }

    @Override
    public boolean supportsFeatureExtraction() {
        return true;
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
    public PacketFeatureExtractor getFeatureExtractor()   {
        return featurePacket -> {
            PacketFeatures features = new PacketFeatures(featurePacket.getCaptureTimestamp());
            return features;
        };
    }

    @Override
    public void resetAfterWindow()
    {
        flowId = null;
        flowChanges = 0;
        lastTimestamp = -1L;
        iatCount = 0;
        encoded.setLength(0);
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