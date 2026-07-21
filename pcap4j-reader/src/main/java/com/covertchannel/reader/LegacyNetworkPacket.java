package com.covertchannel.reader;

import java.io.Serializable;
import java.time.*;
import java.time.temporal.ChronoField;

/**
 * Represents a single network packet extracted from PCAP data.
 * 
 * Contains packet header information and payload data for further analysis.
 */
public class LegacyNetworkPacket implements Serializable {

    //private static final long serialVersionUID = 1L;

    private long timestamp;           // Milliseconds since epoch
    private int packetLength;         // Length of packet in bytes
    private int capturedLength;       // Actual captured bytes
    
    // Layer 3 (IP)
    private String sourceIP;
    private String destIP;
    private String protocol;          // TCP, UDP, ICMP, etc.
    
    // Layer 4 (Transport)
    private int sourcePort;
    private int destPort;
    
    // Data
    private byte[] payloadData;
    private int payloadLength;

    public LegacyNetworkPacket() {
    }

    /**
     * Create a packet with basic info
     */
    public LegacyNetworkPacket(Instant timestamp, String sourceIP, String destIP,
                               int sourcePort, int destPort, String protocol) {
        this.timestamp = timestamp.getLong(ChronoField.MILLI_OF_SECOND);
        this.sourceIP = sourceIP;
        this.destIP = destIP;
        this.sourcePort = sourcePort;
        this.destPort = destPort;
        this.protocol = protocol;
    }

    // ========== Getters & Setters ==========
    
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getPacketLength() {
        return packetLength;
    }

    public void setPacketLength(int packetLength) {
        this.packetLength = packetLength;
    }

    public int getCapturedLength() {
        return capturedLength;
    }

    public void setCapturedLength(int capturedLength) {
        this.capturedLength = capturedLength;
    }

    public String getSourceIP() {
        return sourceIP;
    }

    public void setSourceIP(String sourceIP) {
        this.sourceIP = sourceIP;
    }

    public String getDestIP() {
        return destIP;
    }

    public void setDestIP(String destIP) {
        this.destIP = destIP;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getSourcePort() {
        return sourcePort;
    }

    public void setSourcePort(int sourcePort) {
        this.sourcePort = sourcePort;
    }

    public int getDestPort() {
        return destPort;
    }

    public void setDestPort(int destPort) {
        this.destPort = destPort;
    }

    public byte[] getPayloadData() {
        return payloadData;
    }

    public void setPayloadData(byte[] payloadData) {
        this.payloadData = payloadData;
        this.payloadLength = payloadData != null ? payloadData.length : 0;
    }

    public int getPayloadLength() {
        return payloadLength;
    }

    /**
     * Flow ID is a unique identifier for this packet's flow
     * Format: sourceIP:sourcePort -> destIP:destPort (protocol)
     */
    public String getFlowId() {
        return String.format("%s:%d->%s:%d(%s)", 
            sourceIP, sourcePort, destIP, destPort, protocol);
    }

    @Override
    public String toString() {
        return String.format("Packet[%s:%d -> %s:%d, proto=%s, len=%d, ts=%s]",
            sourceIP, sourcePort, destIP, destPort, protocol, payloadLength, timestamp);
    }

    public void setPayloadLength(int payloadLength) {
        this.payloadLength = payloadLength;
    }
}
