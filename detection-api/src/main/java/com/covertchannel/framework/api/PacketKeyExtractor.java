package com.covertchannel.framework.api;

import com.covertchannel.processor.NetworkPacket;
import org.pcap4j.packet.Packet;
import java.io.Serializable;

/**
 * Lightweight interface for extracting keys from packets.
 * Separated from DetectionAlgorithm to ensure stable serialization in Flink.
 */
@FunctionalInterface
public interface PacketKeyExtractor extends Serializable {
    String getKey(NetworkPacket packet);
}
