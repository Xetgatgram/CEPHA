package com.covertchannel.processor;

import com.covertchannel.framework.AlgorithmJobFactory;
import com.covertchannel.framework.api.*;
import com.covertchannel.processor.PacketFeatures;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * Extrahiert PacketFeatures aus NetworkPackets — einmalig pro Operator-Instanz.
 * Nur aktiv im incremental-Modus (supportsIncrementalAggregation() == true).
 *
 * Der PacketFeatureExtractor wird in open() aus dem Distributed Cache geladen
 * und für die gesamte Lifetime des Operators wiederverwendet.
 * Das NetworkPacket-Objekt kann nach extract() vom GC freigegeben werden.
 */
public class KeyNFeatureExtractorMapFunction extends RichMapFunction<NetworkPacket, PacketFeatures> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(KeyNFeatureExtractorMapFunction.class);

    private final String algorithmClassName;
    private final Map<String, Object> configData;

    private transient PacketKeyExtractor keyExtractor;
    private transient PacketFeatureExtractor featureExtractor;

    public KeyNFeatureExtractorMapFunction(String algorithmClassName, JobContext jobContext) {
        this.algorithmClassName = algorithmClassName;
        this.configData = jobContext.getConfig();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        File jarFile = getRuntimeContext().getDistributedCache()
                .getFile(AlgorithmJobFactory.ALGORITHM_JAR_CACHE_KEY);

        DetectionAlgorithm probe = AlgorithmJobFactory.loadAlgorithmFromJar(
                algorithmClassName, jarFile.getAbsolutePath());
        probe.initialize(new FrameworkConfig(configData));
        this.keyExtractor     = probe.getKeyExtractor();
        this.featureExtractor = probe.getFeatureExtractor();
        probe.close();

        LOG.info("FeatureExtractorMapFunction initialized for {}", algorithmClassName);
    }

    @Override
    public PacketFeatures map(NetworkPacket packet) throws Exception {
        String key = keyExtractor.getKey(packet);
        PacketFeatures features = featureExtractor.extract(packet);
        features.setCustomKey(key);
        packet.setPacketCache(null);
        return features;
    }

    @Override
    public void close() throws Exception {
        // probe.close() wurde bereits in open()
    }
}
