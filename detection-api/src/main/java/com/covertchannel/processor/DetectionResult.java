package com.covertchannel.processor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Result of covert channel detection for a network flow.
 */
public class DetectionResult implements Serializable {

    private static final long serialVersionUID = 2L;

    private String algorithmName;
    private String flowId;
    private long timestamp;

    // data bucket
    // Complex objects (Lists, Strings) will be saved in JSONL for Pandas.
    private Map<String, Object> analysisDetails = new HashMap<>();

    public DetectionResult() {
        this.timestamp = System.currentTimeMillis();
    }

    public DetectionResult(String algorithmName, String flowId) {
        this();
        this.algorithmName = algorithmName;
        this.flowId = flowId;
    }
    /**
     * Fluent API for researchers to add their own keys and values.
     */
    public DetectionResult addDetail(String key, Object value) {
        this.analysisDetails.put(key, value);
        return this;
    }

    // ========== Getters & Setters ==========

    public String getAlgorithmName() { return algorithmName; }
    public void setAlgorithmName(String name) { this.algorithmName = name; }

    public String getFlowId() { return flowId; }
    public void setFlowId(String flowId) { this.flowId = flowId; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public Map<String, Object> getAnalysisDetails() { return analysisDetails; }
    public void setAnalysisDetails(Map<String, Object> details) { this.analysisDetails = details; }


    @Override
    public String toString() {
        return String.format("DetectionResult[%s | %s | %s]",
                algorithmName, flowId, analysisDetails);
    }
}