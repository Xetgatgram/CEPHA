package com.covertchannel.processor;

import com.covertchannel.framework.AlgorithmJobFactory;
import com.covertchannel.framework.api.DetectionAlgorithm;
import com.covertchannel.framework.api.FrameworkConfig;
import com.covertchannel.framework.api.JobContext;
import com.covertchannel.processor.PacketFeatures;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Abstrakte Basisklasse für den Incremental-Detektionspfad.
 *
 * Verarbeitet Iterable<PacketFeatures> direkt — keine AggregateFunction,
 * kein DistributedJarPathHolder. Der Algorithmus wird einmalig in open()
 * pro Operator-Instanz geladen und nach jedem Window via initialize() zurückgesetzt.
 *
 * Lifecycle pro Operator-Instanz:
 *   open()    → JAR laden, algo.initialize(config)
 *   process() → algo.processFlowFeatures(features) → algo.detect() → algo.initialize(config) [reset]
 *   close()   → algo.close()
 *
 * Subklassen implementieren nur window-spezifische Metadaten (TimeWindow vs. GlobalWindow).
 *
 * @param <W> Window-Typ (TimeWindow oder GlobalWindow)
 */
public abstract class AbstractIncrDetectionProcessWIndow<W extends Window>
        extends ProcessWindowFunction<PacketFeatures, DetectionResult, String, W> {

    public static final OutputTag<String> DLQ_TAG =
            new OutputTag<String>("incremental-detection-errors") {};

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(
            AbstractIncrDetectionProcessWIndow.class);

    private final String algorithmClassName;
    private final Map<String, Object> configData;

    // transient — wird in open() auf dem TaskManager geladen
    private transient DetectionAlgorithm algo;

    protected AbstractIncrDetectionProcessWIndow(
            String algorithmClassName, JobContext jobContext) {
        this.algorithmClassName = algorithmClassName;
        this.configData = jobContext.getConfig();
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        File jarFile = getRuntimeContext().getDistributedCache()
                .getFile(AlgorithmJobFactory.ALGORITHM_JAR_CACHE_KEY);
        algo = AlgorithmJobFactory.loadAlgorithmFromJar(
                algorithmClassName, jarFile.getAbsolutePath());
        algo.initialize(new FrameworkConfig(configData));
        LOG.info("IncrementalDetectionProcessWindowFunction initialized for {}", algorithmClassName);
    }

    @Override
    public void close() throws Exception {
        if (algo != null) {
            algo.close();
        }
        super.close();
    }


    @Override
    public void process(String key,
                        Context context,
                        Iterable<PacketFeatures> elements,
                        Collector<DetectionResult> out) {

        int packetsInWindow = 0;
        try {
            // Iterable → List (wird einmalig für processFlowFeatures() benötigt)
            for (PacketFeatures f : elements) {
                packetsInWindow++;
                algo.processFlowFeatures(f);
            }

            DetectionResult result = algo.detect();

            // Reset für nächstes Window
            // falls emit eine Exception wirft, ist der State bereits sauber
            resetAlgo();

            if (result != null) {
                result.setFlowId(key);
                result.addDetail("packets_in_window", String.valueOf(packetsInWindow));
                result.addDetail("aggregation_mode",  "incremental");
                result.addDetail("processing_timestamp",
                        String.valueOf(context.currentProcessingTime()));
                addWindowMetadata(result, context, key);
                out.collect(result);
                LOG.debug("Window processed for key={}, packets={}", key, String.valueOf(packetsInWindow));
            } else {
                LOG.debug("No detection result for key={} (algo returned null)", key);
            }

        } catch (Exception e) {
            resetAlgo(); // State auch im Fehlerfall sauber halten
            String errorMsg = formatErrorMessage(e, key, context);
            context.output(DLQ_TAG, errorMsg);
            LOG.error("Processing failed for key={}, sent to DLQ", key, e);
        }
    }



    private void resetAlgo() {
        try {
            algo.resetAfterWindow();
        } catch (Exception e) {
            LOG.error("Failed to reset algorithm state, operator may be corrupt", e);
        }
    }


    /**
     * Window-spezifische Metadaten zum DetectionResult hinzufügen.
     * TimeWindow:   flink_window_start, flink_window_end
     * GlobalWindow: window_type = "count_based"
     */
    protected abstract void addWindowMetadata(DetectionResult result, Context context, String key);

    /**
     * Fehlermeldung für DLQ formatieren — window-spezifisch.
     */
    protected abstract String formatErrorMessage(Exception e, String key, Context context);
}
