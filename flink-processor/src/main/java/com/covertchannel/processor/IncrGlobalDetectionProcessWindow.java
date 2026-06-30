package com.covertchannel.processor;

import com.covertchannel.framework.api.JobContext;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * Incremental ProcessWindowFunction für COUNT-BASED windows (GlobalWindow).
 *
 * Markiert das Result mit window_type="count_based".
 * Alle Detection-Logik liegt in AbstractIncrementalDetectionProcessWindowFunction.
 */
public class IncrGlobalDetectionProcessWindow
        extends AbstractIncrDetectionProcessWIndow<GlobalWindow> {

    private static final long serialVersionUID = 1L;

    public IncrGlobalDetectionProcessWindow(
            String algorithmClassName, JobContext jobContext) {
        super(algorithmClassName, jobContext);
    }

    @Override
    protected void addWindowMetadata(DetectionResult result, Context context, String key) {
        result.addDetail("window_type", "count_based");
    }

    @Override
    protected String formatErrorMessage(Exception e, String key, Context context) {
        return String.format(
                "{\"error\": \"%s\", \"key\": \"%s\", \"window_type\": \"count_based\"}",
                e.getMessage(), key);
    }
}
