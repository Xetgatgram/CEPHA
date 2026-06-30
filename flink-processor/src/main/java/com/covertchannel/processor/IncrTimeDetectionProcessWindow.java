package com.covertchannel.processor;

import com.covertchannel.framework.api.JobContext;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Incremental ProcessWindowFunction für TIME-BASED windows (TimeWindow).
 *
 * Fügt flink_window_start / flink_window_end als Metadaten hinzu.
 * Alle Detection-Logik liegt in AbstractIncrementalDetectionProcessWindowFunction.
 */
public class IncrTimeDetectionProcessWindow
        extends AbstractIncrDetectionProcessWIndow<TimeWindow> {

    private static final long serialVersionUID = 1L;

    public IncrTimeDetectionProcessWindow(
            String algorithmClassName, JobContext jobContext) {
        super(algorithmClassName, jobContext);
    }

    @Override
    protected void addWindowMetadata(DetectionResult result, Context context, String key) {
        result.addDetail("window_type", "time_based");
        result.addDetail("flink_window_start", String.valueOf(context.window().getStart()));
        result.addDetail("flink_window_end",   String.valueOf(context.window().getEnd()));
    }

    @Override
    protected String formatErrorMessage(Exception e, String key, Context context) {
        return String.format(
                "{\"error\": \"%s\", \"key\": \"%s\", \"window_start\": %d}",
                e.getMessage(), key, context.window().getStart());
    }
}
