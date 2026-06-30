package com.covertchannel.processor;

import org.pcap4j.packet.factory.PacketFactories;
import org.pcap4j.packet.namednumber.DataLinkType;
import org.pcap4j.packet.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Represents a network packet with lazy deserialization support.
 *
 * <p>This class provides an efficient wrapper around raw packet data that minimizes
 * serialization overhead when transmitting packets through Kafka and Flink streams.
 * Instead of serializing the full Pcap4j Packet object (which is expensive), only
 * the raw bytes and metadata are transmitted, and the Packet is reconstructed on-demand.
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li><b>Lazy Deserialization:</b> Pcap4j Packet is only parsed when accessed via {@link #getRawPacket()}</li>
 *   <li><b>Efficient Serialization:</b> Only raw bytes and metadata are serialized to Kafka</li>
 *   <li><b>Data Link Type Support:</b> Preserves DLT information for correct packet parsing</li>
 *   <li><b>Timestamp Preservation:</b> Maintains capture timestamp from PCAP file or live capture</li>
 * </ul>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * // Creating a NetworkPacket from PCAP
 * byte[] rawData = packet.getRawData();
 * int dlt = handle.getDlt().value();
 * long timestamp = handle.getTimestamp().getTime();
 * NetworkPacket netPacket = new NetworkPacket(rawData, dlt, timestamp);
 *
 * // Send to Kafka (only rawData, dlt, timestamp are serialized)
 * producer.send(netPacket);
 *
 * // In Flink operator, parse on-demand
 * Packet packet = netPacket.getRawPacket(); // Lazy parsing happens here
 * IpV4Packet ip = packet.get(IpV4Packet.class);
 * }</pre>
 *
 * @see org.pcap4j.packet.Packet
 * @see org.pcap4j.packet.namednumber.DataLinkType
 * @since 1.0.0
 */
public class NetworkPacket implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(NetworkPacket.class);

    /** Raw packet bytes as captured from network or PCAP file */
    private byte[] rawPacketData;

    /** Data Link Type ID for correct packet parsing (e.g., Ethernet=1, Linux SLL=113) */
    private int dltId;

    /** Capture timestamp in milliseconds since epoch */
    private long captureTimestamp;

    /**
     * Cached parsed Packet object (transient - not serialized).
     * Parsed lazily on first access to minimize memory overhead.
     */
    private transient Packet packetCache;

    /** Custom grouping key assigned by algorithm for packet partitioning */
    private String customKey;

    /**
     * Default constructor for deserialization frameworks.
     */
    public NetworkPacket() {}

    /**
     * Creates a NetworkPacket with raw packet data and metadata.
     *
     * @param rawPacketData Raw packet bytes as captured (must not be null)
     * @param dltId Data Link Type ID (e.g., 1 for Ethernet, 113 for Linux SLL)
     * @param captureTimestamp Capture timestamp in milliseconds since epoch
     */
    public NetworkPacket(byte[] rawPacketData, int dltId, long captureTimestamp) {
        this.dltId = dltId;
        this.rawPacketData = rawPacketData;
        this.captureTimestamp = captureTimestamp;
    }

    /**
     * Gets the cached Packet object without triggering lazy parsing.
     *
     * @return Cached Packet object, or null if not yet parsed
     */
    public Packet getPacketCache() {
        return packetCache;
    }

    /**
     * Sets the cached Packet object (used internally for optimization).
     *
     * @param packetCache Parsed Packet object to cache
     */
    public void setPacketCache(Packet packetCache) {
        this.packetCache = packetCache;
    }

    /**
     * Gets the Data Link Type ID.
     *
     * @return DLT ID (e.g., 1 for Ethernet, 113 for Linux SLL)
     */
    public int getDltId() {
        return dltId;
    }

    /**
     * Sets the Data Link Type ID.
     *
     * @param dltId DLT ID to set
     */
    public void setDltId(int dltId) {
        this.dltId = dltId;
    }

    /**
     * Gets the raw packet data bytes.
     *
     * @return Raw packet bytes as captured
     */
    public byte[] getRawPacketData() {
        return rawPacketData;
    }

    /**
     * Sets the raw packet data bytes.
     *
     * @param rawPacketData Raw packet bytes to set
     */
    public void setRawPacketData(byte[] rawPacketData) {
        this.rawPacketData = rawPacketData;
    }

    /**
     * Gets the capture timestamp.
     *
     * @return Timestamp in milliseconds since epoch
     */
    public long getCaptureTimestamp() {
        return captureTimestamp;
    }

    /**
     * Sets the capture timestamp.
     *
     * @param captureTimestamp Timestamp in milliseconds since epoch
     */
    public void setCaptureTimestamp(long captureTimestamp) {
        this.captureTimestamp = captureTimestamp;
    }

    /**
     * Gets the custom partitioning key.
     *
     * @return Custom key for packet grouping, or null if not set
     */
    public String getCustomKey() {
        return customKey;
    }

    /**
     * Sets the custom partitioning key for packet grouping.
     *
     * @param customKey Custom key to use for partitioning
     */
    public void setCustomKey(String customKey) {
        this.customKey = customKey;
    }

    /**
     * Lazily parses and returns the Pcap4j Packet object.
     *
     * <p>This method implements lazy deserialization to optimize performance:
     * <ul>
     *   <li>First call: Parses raw bytes into Pcap4j Packet (CPU overhead)</li>
     *   <li>Subsequent calls: Returns cached Packet (no overhead)</li>
     *   <li>Serialization: Only raw bytes are transmitted (minimal overhead)</li>
     * </ul>
     *
     * <p><b>Performance Characteristics:</b>
     * <ul>
     *   <li>Serialization overhead: ~0 (only raw bytes transmitted)</li>
     *   <li>Deserialization overhead: Paid once per packet per JVM</li>
     *   <li>Memory overhead: Packet object cached in memory (transient)</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b> Not thread-safe. Do not call concurrently.
     *
     * @return Parsed Pcap4j Packet object, or null if parsing fails
     * @see org.pcap4j.packet.Packet
     * @see org.pcap4j.packet.factory.PacketFactories
     */
    public Packet getRawPacket() {
        if (packetCache == null && rawPacketData != null) {
            try {
                DataLinkType dlt = DataLinkType.getInstance(dltId);
                packetCache = PacketFactories.getFactory(Packet.class, DataLinkType.class)
                        .newInstance(rawPacketData, 0, rawPacketData.length, dlt);
            } catch (Exception e) {
                // Parsing failed - return null (caller should handle gracefully)
                LOG.warn("Failed to parse Pcap4j Packet (DLT={}, Len={}): {}. Check if pcap4j-packetfactory-static is in classpath.",
                        dltId, rawPacketData.length, e.getMessage());
                LOG.debug("Full parsing error", e);
                // Common causes: malformed packet, unsupported protocol, incorrect DLT, missing pcap4j dependencies
                return null;
            }
        }
        return packetCache;
    }
}
