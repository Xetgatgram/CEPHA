package com.covertchannel.processor;

import com.covertchannel.framework.api.DetectionAlgorithm;
import com.covertchannel.framework.AlgorithmJobFactory;
import com.covertchannel.framework.api.FrameworkConfig;
import com.covertchannel.framework.api.JobContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * Generic Detection FlatMap Function.
 * Handles Per-Packet Processing (No Windowing).
 *
 * Uses Flink's Distributed Cache to load the algorithm JAR on TaskManagers.
 */
public class DetectionMapFunction extends RichFlatMapFunction<NetworkPacket, DetectionResult> {

    private static final Logger LOG = LoggerFactory.getLogger(DetectionMapFunction.class);
    private static final long serialVersionUID = 1L;

    private final String algorithmClassName;
    private final Map<String, Object> configData;

    private transient DetectionAlgorithm detector;
    private transient boolean isInitialized = false;


    public DetectionMapFunction(String algorithmClassName, JobContext config) {
        this.algorithmClassName = algorithmClassName;
        this.configData = config.getConfig();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (detector == null) {
            // Resolve JAR from Flink's Distributed Cache local copy on this TaskManager
            File jarFile = getRuntimeContext().getDistributedCache().getFile(AlgorithmJobFactory.ALGORITHM_JAR_CACHE_KEY);
            String localJarPath = jarFile.getAbsolutePath();
            LOG.info("Initializing Map Detector: {} from Distributed Cache: {}", algorithmClassName, localJarPath);

            // 1. Load Algorithm
            detector = AlgorithmJobFactory.loadAlgorithmFromJar(algorithmClassName, localJarPath);
            detector.initialize(new FrameworkConfig(configData));

            isInitialized = true;

            LOG.info("Map Detector initialized successfully.");
        }
    }

    @Override
    public void flatMap(NetworkPacket packet, Collector<DetectionResult> out) throws Exception {
        if (!isInitialized) {
            throw new RuntimeException("Detector not initialized! open() failed or wasn't called.");
        }

        try {
            detector.processFlow(packet);
            DetectionResult result = detector.detect();
            if (result != null) {
                result.setFlowId(packet.getCustomKey());
                out.collect(result);
            }
        } catch (Exception e) {
            LOG.error("Error in detection flatmap function for packet: {}", e.getMessage(), e);
        }
    }


    @Override
    public void close() throws Exception {
        if (detector != null) {
            detector.close();
        }
    }
}