package com.covertchannel.processor;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom Trigger for Count-Based Windows in BATCH Mode (Bounded Streams).
 *
 * PURPOSE:
 * Ensures the last incomplete window fires when a bounded stream ends.
 *
 * PROBLEM:
 * - Default CountTrigger only fires when count threshold is reached
 * - In BATCH mode, the final window may have fewer elements than maxCount
 * - Without this trigger, those final elements are lost (window never fires)
 *
 * SOLUTION:
 * a) Fires when element count reaches maxCount (normal behavior)
 * b) Fires when MAX_WATERMARK is received (end of bounded stream)
 *
 * This approach is deterministic and doesn't rely on processing-time timeouts.
 * Bounded streams automatically emit MAX_WATERMARK when finished.
 *
 * USAGE (BATCH mode with bounded streams):
 * keyedStream
 *   .countWindow(size)
 *   .trigger(new PartialCountWindowBatchTrigger(size))
 *   .aggregate(...)
 *
 * @param <W> Window type (GlobalWindow for count windows)
 */
public class PartialCountWindowBatchTrigger<W extends GlobalWindow> extends Trigger<Object, W> {

    private static final Logger LOG = LoggerFactory.getLogger(PartialCountWindowBatchTrigger.class);
    private static final long serialVersionUID = 1L;

    private final long maxCount;
    private final ReducingStateDescriptor<Long> countStateDesc;

    /**
     * Creates trigger for bounded streams with count windows.
     *
     * @param maxCount Count threshold (must match countWindow size)
     */
    public PartialCountWindowBatchTrigger(long maxCount) {
        if (maxCount <= 0) {
            throw new IllegalArgumentException("maxCount must be > 0");
        }

        this.maxCount = maxCount;

        this.countStateDesc = new ReducingStateDescriptor<>(
            "count-with-eos-trigger",
            new Sum(),
            LongSerializer.INSTANCE
        );

        LOG.info("PartialCountWindowBatchTrigger initialized: maxCount={}", maxCount);
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);

        // Register event-time timer on first element to catch MAX_WATERMARK
        // GlobalWindow.maxTimestamp() returns Long.MAX_VALUE
        if (count.get() == null) {
            long maxTimestamp = window.maxTimestamp();
            ctx.registerEventTimeTimer(maxTimestamp);
            LOG.debug("Registered event-time timer for MAX_WATERMARK: {}", maxTimestamp);
        }

        count.add(1L);
        long currentCount = count.get();

        // Normal count-based firing
        if (currentCount >= maxCount) {
            LOG.debug("Count threshold reached: {}/{} - FIRE_AND_PURGE", currentCount, maxCount);
            count.clear();
            return TriggerResult.FIRE_AND_PURGE;
        }

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        // When bounded stream ends, Flink emits MAX_WATERMARK (Long.MAX_VALUE)
        // This fires the event-time timer we registered at window.maxTimestamp()

        ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);
        Long currentCount = count.get();

        if (currentCount != null && currentCount > 0) {
            LOG.info("MAX_WATERMARK received: firing incomplete window with {} elements (threshold: {})",
                     currentCount, maxCount);
            count.clear();
            return TriggerResult.FIRE_AND_PURGE;
        }

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        // Not used - we rely on event time (watermarks)
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(countStateDesc).clear();
        // Event-time timer is automatically cleaned up when window state is cleared
    }

    @Override
    public String toString() {
        return String.format("PartialCountWindowBatchTrigger(maxCount=%d)", maxCount);
    }


    // state reducer


    private static class Sum implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) {
            return value1 + value2;
        }
    }
}
