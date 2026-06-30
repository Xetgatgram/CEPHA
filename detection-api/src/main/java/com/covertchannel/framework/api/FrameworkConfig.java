package com.covertchannel.framework.api;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration holder for framework and algorithm parameters.
 */
public class FrameworkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, Object> config;

    public FrameworkConfig() {
        this.config = new HashMap<>();
    }

    public FrameworkConfig(Map<String, Object> initialConfig) {
        this.config = new HashMap<>(initialConfig);
    }

    public void set(String key, Object value) {
        config.put(key, value);
    }

    public Object get(String key) {
        return config.get(key);
    }

    public String getString(String key, String defaultValue) {
        Object value = config.get(key);
        return value != null ? value.toString() : defaultValue;
    }

    public int getInt(String key, int defaultValue) {
        Object value = config.get(key);
        return value != null ? Integer.parseInt(value.toString()) : defaultValue;
    }

    public double getDouble(String key, double defaultValue) {
        Object value = config.get(key);
        return value != null ? Double.parseDouble(value.toString()) : defaultValue;
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        Object value = config.get(key);
        return value != null ? Boolean.parseBoolean(value.toString()) : defaultValue;
    }

    public Map<String, Object> getAllConfig() {
        return new HashMap<>(config);
    }
}