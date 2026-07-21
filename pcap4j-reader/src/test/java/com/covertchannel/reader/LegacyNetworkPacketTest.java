package com.covertchannel.reader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for NetworkPacket class
 */
@DisplayName("NetworkPacket Tests")
public class LegacyNetworkPacketTest {

    @Test
    @DisplayName("Should create packet with basic information")
    public void testCreatePacket() {
        LegacyNetworkPacket packet = new LegacyNetworkPacket(
            Instant.EPOCH,
            "192.168.1.1",
            "192.168.1.100",
            12345,
            443,
            "TCP"
        );

        assertEquals(Instant.EPOCH, packet.getTimestamp());
        assertEquals("192.168.1.1", packet.getSourceIP());
        assertEquals("192.168.1.100", packet.getDestIP());
        assertEquals(12345, packet.getSourcePort());
        assertEquals(443, packet.getDestPort());
        assertEquals("TCP", packet.getProtocol());
    }

    @Test
    @DisplayName("Should generate correct flow ID")
    public void testFlowId() {
        LegacyNetworkPacket packet = new LegacyNetworkPacket(
                Instant.EPOCH,
            "10.0.0.1",
            "10.0.0.2",
            5000,
            80,
            "TCP"
        );

        String flowId = packet.getFlowId();
        assertEquals("10.0.0.1:5000->10.0.0.2:80(TCP)", flowId);
    }

    @Test
    @DisplayName("Should set and get payload data")
    public void testPayloadData() {
        LegacyNetworkPacket packet = new LegacyNetworkPacket();
        byte[] payload = new byte[]{1, 2, 3, 4, 5};
        
        packet.setPayloadData(payload);
        
        assertArrayEquals(payload, packet.getPayloadData());
        assertEquals(5, packet.getPayloadLength());
    }

    @Test
    @DisplayName("Should handle null payload")
    public void testNullPayload() {
        LegacyNetworkPacket packet = new LegacyNetworkPacket();
        packet.setPayloadData(null);
        
        assertNull(packet.getPayloadData());
        assertEquals(0, packet.getPayloadLength());
    }

    @Test
    @DisplayName("Should generate meaningful string representation")
    public void testToString() {
        LegacyNetworkPacket packet = new LegacyNetworkPacket(
                Instant.EPOCH,
            "192.168.1.1",
            "10.0.0.1",
            54321,
            22,
            "SSH"
        );

        String str = packet.toString();
        assertTrue(str.contains("192.168.1.1"));
        assertTrue(str.contains("10.0.0.1"));
        assertTrue(str.contains("SSH"));
    }
}
