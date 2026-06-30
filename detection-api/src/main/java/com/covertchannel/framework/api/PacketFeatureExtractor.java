package com.covertchannel.framework.api;

import com.covertchannel.processor.NetworkPacket;
import com.covertchannel.processor.PacketFeatures;

import java.io.Serializable;

@FunctionalInterface
public interface PacketFeatureExtractor extends Serializable {
    PacketFeatures extract(NetworkPacket packet);
}