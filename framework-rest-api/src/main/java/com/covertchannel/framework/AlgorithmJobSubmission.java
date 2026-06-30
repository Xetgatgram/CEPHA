package com.covertchannel.framework;

import java.util.Map;

/**
 * AlgorithmJobSubmission
 * 
 * DTO für Submission
 * Enthält notwendigen Parameter
 * 
 * Window Konfiguration :
 *  windowType: Art des Fensters TUMBLING, SLIDING, SESSION
 *  windowSizeMs: Größe des Fensters in Millisekunden
 *  slideMs: Versatz für SLIDING_WINDOW
 */
public class AlgorithmJobSubmission {
    
    private final String algorithmId;
    private final String jarPath;
    private final Map<String, Object> configuration;


    // Window Konfiguration


    /**
     * Constructor mit  Parametern und Window-Config
     */
    public AlgorithmJobSubmission(String algorithmId, String jarPath, Map<String, Object> configuration) {
        this.algorithmId = algorithmId;
        this.jarPath = jarPath;
        this.configuration = configuration;
    }


    public String getAlgorithmId() { return algorithmId; }
    public String getJarPath() { return jarPath; }
    public Map<String, Object> getConfiguration() { return configuration; }

}

