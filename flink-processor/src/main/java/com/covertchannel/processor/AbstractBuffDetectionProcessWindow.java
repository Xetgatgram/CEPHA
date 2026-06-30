package com.covertchannel.processor;

import com.covertchannel.framework.AlgorithmJobFactory;
import com.covertchannel.framework.api.DetectionAlgorithm;
import com.covertchannel.framework.api.FrameworkConfig;
import com.covertchannel.framework.api.JobContext;
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
 * Abstrakte Basisklasse für den Buffered-Detektionspfad.
 *
 * Verarbeitet Iterable<NetworkPacket> direkt — keine AggregateFunction,
 * kein DistributedJarPathHolder. Flink puffert die NetworkPackets im
 * Window-State bis das Window feuert.
 *
 * Lifecycle pro Operator-Instanz:
 *   open()    → JAR laden, algo.initialize(config)
 *   process() → algo.processFlowBatch(packets) → algo.detect() → algo.resetAfterWindow()
 *   close()   → algo.close()
 *
 * @param <W> Window-Typ (TimeWindow oder GlobalWindow)
 */
public abstract class AbstractBuffDetectionProcessWindow<W extends Window>
        extends ProcessWindowFunction<NetworkPacket, DetectionResult, String, W> {

    public static final OutputTag<String> DLQ_TAG =
            new OutputTag<String>("buffered-detection-errors") {};

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(
            AbstractBuffDetectionProcessWindow.class);

    private final String algorithmClassName;
    private final Map<String, Object> configData;

    private transient DetectionAlgorithm algo;

    protected AbstractBuffDetectionProcessWindow(
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
        LOG.info("BufferedDetectionProcessWindowFunction initialized for {}", algorithmClassName);
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
                        Iterable<NetworkPacket> elements,
                        Collector<DetectionResult> out) {
        int packetsInWindow = 0;
        try {
            for (NetworkPacket packets : elements) {
                packetsInWindow++;
                algo.processFlow(packets);
            }

            DetectionResult result = algo.detect();

            resetAlgo();

            if (result != null) {
                result.setFlowId(key);
                result.addDetail("packets_in_window", String.valueOf(packetsInWindow));
                result.addDetail("aggregation_mode","buffered");
                result.addDetail("processing_timestamp", String.valueOf(context.currentProcessingTime()));
                addWindowMetadata(result, context, key);
                out.collect(result);
                LOG.debug("Window processed for key={}, packets={}", key);
            } else {
                LOG.debug("No detection result for key={} (algo returned null)", key);
            }

        } catch (Exception e) {
            resetAlgo();
            context.output(DLQ_TAG, formatErrorMessage(e, key, context));
            LOG.error("Processing failed for key={}, sent to DLQ", key, e);
        }
    }

    // Reset

    private void resetAlgo() {
        try {
            algo.resetAfterWindow();
        } catch (Exception e) {
            LOG.error("resetAfterWindow() failed for {} — operator may be corrupt",
                    algorithmClassName, e);
        }
    }

    // Abstrakte Methoden

    protected abstract void addWindowMetadata(DetectionResult result, Context context, String key);

    protected abstract String formatErrorMessage(Exception e, String key, Context context);
}
