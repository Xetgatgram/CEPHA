package com.covertchannel.processor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
/**
 * Lightweight, serializable feature container extracted from a raw NetworkPacket.
 *
 * Used in AggregateFunction for Accumulation

 * Usage (in extract()):
 *   return new PacketFeatures(packet.getCaptureTimestamp());
 *   return new PacketFeatures(packet.getCaptureTimestamp())
 *       .add("ip_id",extractIpId(packet))
 *       .add("ttl",extractTtl(packet));
 */
public class PacketFeatures implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private final long captureTimestamp;

    // Routing key for grouping packets into Flows
    private String customKey;

    // Serializable so the compiler enforces
    // that all values are safe for Flink checkpointing.
    // Lazy init: null until first add() call
    private Map<String, Serializable> features;
    
    public PacketFeatures() {
        captureTimestamp = 0;
    }

    public PacketFeatures(long captureTimestamp) {
        this.captureTimestamp = captureTimestamp;
    }

    /**
     * Fluent API 
     */
    public PacketFeatures add(String key, Serializable value) {
        if (this.features == null) {
            this.features = new HashMap<>();
        }
        this.features.put(key, value);
        return this;
    }

    // Getters 

    public long getCaptureTimestamp() {
        return captureTimestamp;
    }

    public Serializable get(String key) {
        return features != null ? features.get(key) : null;
    }

    public long getLong(String key, long defaultValue) {
        Serializable v = get(key);
        return v instanceof Long ? (Long) v : defaultValue;
    }

    public int getInt(String key, int defaultValue) {
        Serializable v = get(key);
        return v instanceof Integer ? (Integer) v : defaultValue;
    }

    public double getDouble(String key, double defaultValue) {
        Serializable v = get(key);
        return v instanceof Double ? (Double) v : defaultValue;
    }

    public String getString(String key, String defaultValue) {
        Serializable v = get(key);
        return v instanceof String ? (String) v : defaultValue;
    }

    @Override
    public String toString() {
        return "PacketFeatures{ts=" + captureTimestamp +
                (features != null ? ", " + features : "") + "}";
    }
    //------------------------ Routing --------------------------


    public String getCustomKey() {
        return customKey;
    }
    /**
     * Routing key for grouping packets
     * Do NOT touch this value, it is set by the framework
     * @param customKey
     */
    public void setCustomKey(String customKey) {
        this.customKey = customKey;
    }
}

