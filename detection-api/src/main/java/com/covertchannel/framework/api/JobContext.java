package com.covertchannel.framework.api;


import java.util.Map;

public class JobContext {


    // Core Attributes
    private final String algorithmClassName;
    private final String kafkaBrokers;
    private final String inputTopic;
    private final String outputTopic;

    // Windowing Attributes Explicitly typed the Factory needs them
    private final String windowType;
    private final long windowSizeMs;
    private final long slideMs;

    // Count based window attributes
    private final long windowCount;
    private final long slideCount;

    // other (For algorithm-specific logic)
    private final Map<String, Object> rawConfig;

    public JobContext(Map<String, Object> map) {
        this.rawConfig = map;

        // Extracting core values with defaults/validation
        this.algorithmClassName = (String) map.get("algorithmClassName");
        this.kafkaBrokers = (String) map.get("kafkaBrokers");
        this.inputTopic = (String) map.get("inputTopic");
        this.outputTopic = (String) map.get("outputTopic");

        this.windowType = (String) map.getOrDefault("windowType", "tumbling");
        this.windowSizeMs = castToLong(map.getOrDefault("windowSizeMs", 5000L));
        this.slideMs = castToLong(map.getOrDefault("slideMs", 0L));

        this.windowCount = castToLong(map.getOrDefault("windowCount", 0L));
        this.slideCount = castToLong(map.getOrDefault("slideCount", 0L));
    }

    // Helper to handle JSON number conversion (Integer vs Long)
    private long castToLong(Object obj) {
        if (obj instanceof Number) return ((Number) obj).longValue();
        return Long.parseLong(obj.toString());
    }

    //  GETTER
    public String getAlgorithmClassName() { return algorithmClassName; }
    public String getKafkaBrokers() { return kafkaBrokers; }
    public String getInputTopic() {return inputTopic;}
    public String getOutputTopic() {return outputTopic;}
    public String getWindowType() {return windowType;}
    public long getWindowSizeMs() {return windowSizeMs;}
    public long getSlideMs() {return slideMs;}
    public long getWindowCount() {return windowCount;}
    public long getSlideCount() {return slideCount;}


    /**
     * Allows an algorithm to get its specific settings like "threshold"
     */
    public Object getParam(String key) {
        return rawConfig.get(key);
    }

    public Map<String, Object> getConfig() {
        return rawConfig;
    }

}