
package com.covertchannel.processor;

import com.covertchannel.framework.api.JobContext;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Buffered ProcessWindowFunction für TIME-BASED windows.
 */
public class BuffTimeDetectionProcessWindow
        extends AbstractBuffDetectionProcessWindow<TimeWindow> {

    private static final long serialVersionUID = 1L;

    public BuffTimeDetectionProcessWindow(
            String algorithmClassName, JobContext jobContext) {
        super(algorithmClassName, jobContext);
    }

    @Override
    protected void addWindowMetadata(DetectionResult result, Context context, String key) {
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
