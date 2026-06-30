package com.covertchannel.framework;

import com.covertchannel.framework.api.*;
import com.covertchannel.processor.*;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.UUID;

/**
 * AlgorithmJobFactory
 * 
 * Creates Flink StreamGraphs for dynamic execution of detection algorithms.
 * Each algorithm runs as an isolated Flink Job.
 *
 * JAR distribution uses Flink's Distributed Cache: the JAR is registered once
 * on the client side and automatically shipped to all TaskManagers via the BlobStore.
 * TWO PIPELINE MODES:
 *
 *BUFFERED  (supportsFeatureExtraction() == false):
 *  filter > KeyExtractorMapFunction > keyBy > window > process(Buffered[Time|Global]DetectionProcessWindowFunction)
 *
 * INCREMENTAL (supportsFeatureExtraction() == true):
 *   filter > KeyExtractorMapFunction > keyBy > FeatureExtractorMapFunction> window > process(Incremental[Time|Global]DetectionProcessWindowFunction)
 *
 * In both modes the algorithm JAR is loaded once per TaskManager slot in open(),
 * reused for the lifetime of the operator, and reset between windows with
 * DetectionAlgorithm.resetAfterWindow().
 */
public class AlgorithmJobFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AlgorithmJobFactory.class);

    /** Key used to register the algorithm JAR in Flink's Distributed Cache. */
    public static final String ALGORITHM_JAR_CACHE_KEY = "algorithm-jar";

    private static final java.util.Map<String, CachedClassLoader> classLoaderCache = new java.util.concurrent.ConcurrentHashMap<>();

    private static final class CachedClassLoader {
        final URLClassLoader classLoader;
        final long jarLastModified;

        CachedClassLoader(URLClassLoader classLoader, long jarLastModified) {
            this.classLoader = classLoader;
            this.jarLastModified = jarLastModified;
        }
    }

    public enum WindowType {
        NONE("none"),
        TUMBLING_WINDOW("tumbling"),
        SLIDING_WINDOW("sliding"),
        SESSION_WINDOW("session"),
        TUMBLING_COUNT("tumbling_count"),
        SLIDING_COUNT("sliding_count");

        private final String configValue;
        WindowType(String configValue) { this.configValue = configValue; }

        public static WindowType fromConfig(String value) {
            if (value == null) return TUMBLING_WINDOW;
            for (WindowType type : WindowType.values()) {
                if (type.configValue.equalsIgnoreCase(value.trim())) return type;
            }
            return TUMBLING_WINDOW;
        }

        public boolean isCountBased() {
            return this == TUMBLING_COUNT || this == SLIDING_COUNT;
        }
    }

    /**
     * Creates a complete Flink Job Graph for a detection algorithm.
     */
    public static StreamGraph createJobGraph(String algorithmJarPath, JobContext context) throws Exception {
        // Check for Execution Mode in Config (BATCH vs STREAMING) default is STREAMING
        String execMode = context.getConfig().getOrDefault("executionMode", "STREAMING").toString().toUpperCase();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Register the algorithm JAR in Flink's Distributed Cache
        // This ships the JAR from the client (REST-API / JobManager) to all TaskManagers
        // automatically via the BlobStore. TaskManagers retrieve it with getDistributedCache().
        env.registerCachedFile(algorithmJarPath, ALGORITHM_JAR_CACHE_KEY);
        LOG.info("Registered algorithm JAR in Distributed Cache: {}", algorithmJarPath);

        //Check if PacketFeatures should get extracted
        boolean incremental = isIncremental(context, algorithmJarPath);

        if ("BATCH".equals(execMode)) {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            LOG.info("Configuring Job for BATCH execution mode.");
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.setStateBackend(new HashMapStateBackend());
            env.getCheckpointConfig().setCheckpointStorage(
                    "file://" + System.getenv().getOrDefault(
                            "TASKMANAGER_SPILL_DIR", "/tmp"));
        }

        // Set Parallelism if configured
        if (context.getConfig().containsKey("parallelism")) {
            int parallelism = Integer.parseInt(context.getConfig().get("parallelism").toString());
            env.setParallelism(parallelism);
            LOG.info("Job Parallelism set to: {}", parallelism);
        }

        env.getConfig().registerKryoType(DetectionResult.class);
        env.getConfig().registerKryoType(NetworkPacket.class);
        env.getConfig().registerKryoType(PacketFeatures.class);

        //  Configure Source (Kafka)
        DataStream<NetworkPacket> packetStream = createKafkaSource(env, context, execMode);

        //  Keying Logic (Group by Flow)
        //KeyedStream<NetworkPacket, String> keyedStream = createKeyedStream(packetStream, context.getAlgorithmClassName());

        //  Detection Logic (Windowing + Processing)
        SingleOutputStreamOperator<DetectionResult> detectionStream = createDetectionPipeline(packetStream, context, incremental,execMode);

        //  Output Configuration (Results & Errors)
        configureSinks(detectionStream, incremental,context,execMode);

        //  Finalize Graph
        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setJobName("Detector-" + context.getAlgorithmClassName());
        
        return streamGraph;
    }
// Helpers

    /**
     * Checks if the algorithm supports FeatureExtraction.
     * @param context
     * @param algorithmJarPath
     * @return true if the algorithm supports FeatureExtraction  , false otherwise.
     */
        private static boolean isIncremental(JobContext context, String algorithmJarPath){
        boolean isIncremental = false;
        try {
            DetectionAlgorithm detector = loadAlgorithmFromJar(context.getAlgorithmClassName(), algorithmJarPath);
            detector.initialize(new FrameworkConfig(context.getConfig()));
            isIncremental = detector.supportsFeatureExtraction();
            detector.close();

        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("Could not probe algorithm mode, defaulting to BUFFERED: {}", e.getMessage());
        }
        return isIncremental;
    }

    private static DataStream<NetworkPacket> createKafkaSource(StreamExecutionEnvironment env, JobContext context, String execMode) {
        String algorithmName = context.getAlgorithmClassName();
        String kafkaBrokers = System.getenv("KAFKA_BOOTSTRAPSERVERS");

        if (kafkaBrokers == null || kafkaBrokers.isEmpty()) {
            kafkaBrokers = context.getKafkaBrokers();
        }

        KafkaSourceBuilder<NetworkPacket> builder = KafkaSource.<NetworkPacket>builder()
                .setBootstrapServers(kafkaBrokers)
                .setProperty("auto.offset.reset", "earliest")
                .setProperty("max.poll.records", "500")
                .setProperty("fetch.max.bytes", "5242880")      // 5 MB pro Fetch
                //.setProperty("max.partition.fetch.bytes", "1048576")  // 1 MB pro Partition
                .setTopics(context.getInputTopic())
                .setValueOnlyDeserializer(new PcapPacketDeserializer());

                 if ("BATCH".equals(execMode)) {
                    LOG.info("Setting Kafka Source to BOUNDED (latest offsets) for Batch processing.");
                     String groupId = "covert-batch-" + algorithmName.hashCode() + "-" + UUID.randomUUID().hashCode();
                     builder.setGroupId(groupId)
                             .setStartingOffsets(OffsetsInitializer.earliest())
                             .setBounded(OffsetsInitializer.latest());
                     LOG.info("BATCH mode: group={}, reading earliest → latest (bounded)", groupId);
                 } else if("REPLAY".equals(execMode)) { //Streaming with bounded offsets
                     String groupId = "covert-replay-" + algorithmName.hashCode() + "-" + UUID.randomUUID().hashCode();
                     builder.setGroupId(groupId)
                             .setStartingOffsets(OffsetsInitializer.earliest())
                             .setBounded(OffsetsInitializer.latest()); //  End-of-Stream Watermark gets emitted at latest offset.
                     LOG.info("REPLAY mode: group={}, reading earliest → latest (bounded, streaming semantics)", groupId);
                 } else {
                     String groupId = "covert-stream-" + algorithmName.hashCode() + "-" + UUID.randomUUID().hashCode();
                     builder.setGroupId(groupId)
                             .setStartingOffsets(OffsetsInitializer.earliest());
                     LOG.info("STREAMING mode: group={}, starting at latest (resumes from committed offset on restart)", groupId);
                 }

            KafkaSource<NetworkPacket> kafkaSource = builder.build();

        return env.fromSource(kafkaSource, WatermarkStrategy
                .<NetworkPacket>forBoundedOutOfOrderness(Duration.ofMillis(10))
                .withTimestampAssigner((networkPacket, recordTimestamp) -> networkPacket.getCaptureTimestamp()),
                "Kafka-Packet-Source");
    }

    private static SingleOutputStreamOperator<DetectionResult> createDetectionPipeline(
            DataStream<NetworkPacket> packetStream,
            JobContext context, boolean incremental, String execMode) {

        WindowType winType = WindowType.fromConfig(context.getWindowType());
        String className = context.getAlgorithmClassName();

        if (winType == WindowType.NONE) {
            LOG.info("Pipeline: PER-PACKET (No Window)");
            return createKeyedStream(packetStream, className)
                    .flatMap(new DetectionMapFunction(className, context))
                    .name("Per-Packet-Detector");
        }
        if(incremental) {
            KeyedStream<PacketFeatures,String> keyedNFeaturedPackets =
                    createKeyedNFeatureStream(packetStream, className, context);

            //Count based windowing
            if (winType.isCountBased()) {
                return buildIncrCountPipeline(keyedNFeaturedPackets, context, execMode);
            }

            //Time based windowing
            return buildIncrTimePipeline(keyedNFeaturedPackets, context);
        }
        else {
            // filter → KeyExtractorMapFunction → keyBy(NetworkPacket::getCustomKey)
            KeyedStream<NetworkPacket, String> keyedPackets =
                    createKeyedStream(packetStream, className);

            //Count based windowing
            if (winType.isCountBased()) {
                return buildCountPipeline(keyedPackets, context, execMode);
            }

            //Time based windowing
            return buildTimePipeline(keyedPackets, context);

        }
    }

    //count Pipeline
    private static SingleOutputStreamOperator<DetectionResult> buildCountPipeline(
            KeyedStream<NetworkPacket, String> keyedPackets,
            JobContext context,
            String execMode) {

        String className = context.getAlgorithmClassName();
        WindowType winType = WindowType.fromConfig(context.getWindowType());
        long count = context.getWindowCount();
        long slideCount = context.getSlideCount();
        validateCountWindowConfig(count, slideCount, winType);

        boolean partialFlush = "BATCH".equals(execMode) &&
                Boolean.parseBoolean(
                        context.getConfig().getOrDefault("partialFlush", "false").toString());

        boolean isTumbling = (slideCount == 0 || slideCount == count);
        LOG.info("Pipeline: COUNT-BASED {} (size={}, slide={}, incremental={}, partialFlush={})",
                isTumbling ? "TUMBLING" : "SLIDING", count, slideCount, "false", partialFlush);


            // BUFFERED: keyBy → countWindow → process
        final WindowedStream windowedStream;
        if (isTumbling) {
            WindowedStream ws = keyedPackets
                    .window(GlobalWindows.create());
            windowedStream = partialFlush
                    ? ws.trigger(new PartialCountWindowBatchTrigger<>(count))
                    : ws.trigger(PurgingTrigger.of(CountTrigger.of(count)));
        } else {
            WindowedStream ws = keyedPackets.countWindow(count, slideCount);
            windowedStream = partialFlush
                    ? ws.trigger(new PartialCountWindowBatchTrigger<>(count))
                    : ws;
        }

            return windowedStream
                    .process(new BuffGlobalDetectionProcessWindow(className, context))
                    .name(isTumbling ? "Count-Tumbling-Buffered-Detector"
                            : "Count-Sliding-Buffered-Detector");

    }
    //incremental pipeline
    private static SingleOutputStreamOperator<DetectionResult> buildIncrCountPipeline(
            KeyedStream<PacketFeatures, String> keyedPackets,
            JobContext context,
            String execMode) {

        String className = context.getAlgorithmClassName();
        WindowType winType = WindowType.fromConfig(context.getWindowType());
        long count = context.getWindowCount();
        long slideCount = context.getSlideCount();
        validateCountWindowConfig(count, slideCount, winType);

        boolean partialFlush = "BATCH".equals(execMode) &&
                Boolean.parseBoolean(
                        context.getConfig().getOrDefault("partialFlush", "false").toString());

        boolean isTumbling = (slideCount == 0 || slideCount == count);
        LOG.info("Pipeline: COUNT-BASED {} (size={}, slide={}, incremental={}, partialFlush={})",
                isTumbling ? "TUMBLING" : "SLIDING", count, slideCount, "true", partialFlush);

            // keyBy > FeatureExtractorMapFunction > countWindow > process
        final WindowedStream windowedStream;
        if (isTumbling) {
            WindowedStream ws = keyedPackets
                    .window(GlobalWindows.create());
            windowedStream = partialFlush
                    ? ws.trigger(new PartialCountWindowBatchTrigger<>(count))
                    : ws.trigger(PurgingTrigger.of(CountTrigger.of(count)));
        } else {
            WindowedStream ws = keyedPackets.countWindow(count, slideCount);
            windowedStream = partialFlush
                    ? ws.trigger(new PartialCountWindowBatchTrigger<>(count))
                    : ws;
        }

            return windowedStream
                    .process(new IncrGlobalDetectionProcessWindow(className, context))
                    .name(isTumbling ? "Count-Tumbling-Incremental-Detector"
                            : "Count-Sliding-Incremental-Detector");

    }

    //time pipeline
    private static SingleOutputStreamOperator<DetectionResult> buildTimePipeline(
            KeyedStream<NetworkPacket, String> keyedPackets,
            JobContext context) {

        String className = context.getAlgorithmClassName();
        WindowType winType = WindowType.fromConfig(context.getWindowType());
        long size = context.getWindowSizeMs();
        long slide = context.getSlideMs();
        validateTimeWindowConfig(size, slide, winType);

        LOG.info("Pipeline: TIME-BASED {} ({} ms, incremental={})", winType, size, "false");

        WindowAssigner<? super NetworkPacket, TimeWindow> assigner =
                createTimeWindowAssigner(winType, size, slide);


            // BUFFERED: keyBy → window → process
            return keyedPackets
                    .window(assigner)
                    .process(new BuffTimeDetectionProcessWindow(className, context))
                    .name("Time-Windowed-Buffered-Detector");

    }

    //time pipeline
    private static SingleOutputStreamOperator<DetectionResult> buildIncrTimePipeline(
            KeyedStream<PacketFeatures, String> keyedPackets,
            JobContext context) {

        String className = context.getAlgorithmClassName();
        WindowType winType = WindowType.fromConfig(context.getWindowType());
        long size = context.getWindowSizeMs();
        long slide = context.getSlideMs();
        validateTimeWindowConfig(size, slide, winType);

        LOG.info("Pipeline: TIME-BASED {} ({} ms, incremental={})", winType, size, "true");

        WindowAssigner<? super NetworkPacket, TimeWindow> assigner =
                createTimeWindowAssigner(winType, size, slide);

            // keyBy > FeatureExtractorMapFunction > window > process
            return keyedPackets
                    .window(createFeatureWindowAssigner(winType, size, slide))
                    .process(new IncrTimeDetectionProcessWindow(className, context))
                    .name("Time-Windowed-Incremental-Detector");
    }


    /**
     * Shared keying logic for non-FeatureExtraction pipeline modes:
     *   filter(not null) > KeyExtractorMapFunction > keyBy(NetworkPacket::getCustomKey)
     *
     * KeyExtractorMapFunction sets packet.customKey via the algorithm's PacketKeyExtractor,
     * loaded once per TaskManager slot from the Distributed Cache.
     */
    private static KeyedStream<NetworkPacket, String> createKeyedStream(
            DataStream<NetworkPacket> stream, String className) {
        return stream
                .filter(packet -> packet != null)
                .map(new KeyExtractorMapFunction(className))
                .keyBy(NetworkPacket::getCustomKey);
    }

    /**
     * Shared keying logic for FeatureExtraction pipeline modes:
     *   filter(not null) > KeyNFeatureExtractorMapFunction > keyBy(PacketFeatures::getCustomKey)
     *
     * KeyNFeatureExtractorMapFunction sets packetFeatures.customKey via the algorithm's PacketKeyExtractor,
     * loaded once per TaskManager slot from the Distributed Cache.
     */
    private static KeyedStream<PacketFeatures, String> createKeyedNFeatureStream(
            DataStream<NetworkPacket> stream, String className, JobContext context) {

        return stream
                .filter(packet -> packet != null)
                .map(new KeyNFeatureExtractorMapFunction(className, context))
                .assignTimestampsAndWatermarks(passWatermark())
                .keyBy(PacketFeatures::getCustomKey);
    }

    /**
     *Creates a WatermarkStrategy for PacketFeatures that passes through upstream
     * watermarks without generating new ones, and assigns Event-Time timestamps
     * from {@link PacketFeatures#getCaptureTimestamp()}.
     *
     * Used in the FeatureExtraction pipeline after KeyNFeatureExtractorMapFunction to ensure
     * Time-Windows receive correct Event-Time metadata from the original NetworkPacket timestamps.
     *
     * @return WatermarkStrategy that passes through timestamps and emits watermarks.
     */
    private static WatermarkStrategy<PacketFeatures> passWatermark() {

        WatermarkStrategy<PacketFeatures> passStrategy = new WatermarkStrategy<PacketFeatures>() {
            @Override
            public WatermarkGenerator<PacketFeatures> createWatermarkGenerator(
                    WatermarkGeneratorSupplier.Context ctx) {
                return new WatermarkGenerator<PacketFeatures>() {
                    @Override public void onEvent(PacketFeatures e, long ts, WatermarkOutput out) {}

                    @Override public void onPeriodicEmit(WatermarkOutput out) {}
                };
            }
            @Override
            public TimestampAssigner<PacketFeatures> createTimestampAssigner(
                    TimestampAssignerSupplier.Context ctx) {
                return (features, recordTimestamp) -> features.getCaptureTimestamp();
            }
        };

        return passStrategy;
    }


    /**
     * Configures result and DLQ sinks.
     *
     * The DLQ OutputTag depends on the pipeline mode:
     *   INCREMENTAL → AbstractIncrementalDetectionProcessWindowFunction.DLQ_TAG
     *   BUFFERED    → AbstractBufferedDetectionProcessWindowFunction.DLQ_TAG
     * For PER-PACKET mode (WindowType.NONE), no DLQ tag exists — DLQ is skipped.
     */
    private static void configureSinks(
            SingleOutputStreamOperator<DetectionResult> stream,
            boolean incremental,
            JobContext context,
            String execMode) {

        // Main result sink (JSONL)
        FileSink<DetectionResult> resultSink =
                DetectionSinkFactory.createJsonlSink(context.getAlgorithmClassName(), execMode);

        if (resultSink != null) {
            if ("BATCH".equals(execMode)) {
                stream.sinkTo(resultSink)
                        .setParallelism(1)
                        .name("Result-File-Sink-Batch");
            } else {
                stream.sinkTo(resultSink)
                        .disableChaining()
                        .setParallelism(1) // stream.getParallelism())
                        .name("Result-File-Sink-Streaming");
            }
        }

        // Dead Letter Queue sink (windowed modes only)
        WindowType winType = WindowType.fromConfig(context.getWindowType());
        if (winType == WindowType.NONE) {
            LOG.debug("PER-PACKET mode — no DLQ sink configured.");
            return;
        }
        try {
            OutputTag<String> dlqTag = incremental ? AbstractIncrDetectionProcessWIndow.DLQ_TAG : AbstractBuffDetectionProcessWindow.DLQ_TAG;

            DataStream<String> dlqStream = stream.getSideOutput(dlqTag);
            String errorBase = System.getenv(CephaConfig.ENV_DLQ_PATH);
            if (errorBase == null || errorBase.isBlank()) {
                LOG.warn("CEPHA_DLQ_PATH not set, skipping DLQ sink.");
                return;
            }
            String errorPath = (errorBase.endsWith("/") ? errorBase : errorBase + "/")
                    + context.getAlgorithmClassName();
            FileSink<String> errorSink = FileSink
                    .forRowFormat(new Path(errorPath), new SimpleStringEncoder<String>("UTF-8"))
                    .build();
            dlqStream.sinkTo(errorSink).name("DLQ-Error-Sink");
            LOG.info("DLQ sink configured at: {}", errorPath);
        } catch (Exception e) {
            LOG.debug("DLQ configuration skipped: {}", e.getMessage());
        }
    }

    //Windowing validation and Assigners -------------------------------------------------------------------------

    private static void validateTimeWindowConfig(Long size, Long slide, WindowType type) {
        if (size == null || size <= 0)
            throw new IllegalArgumentException("Window size must be > 0");
        if (type == WindowType.SLIDING_WINDOW && (slide == null || slide <= 0))
            throw new IllegalArgumentException("Slide size must be > 0 for Sliding Window");
    }

    private static void validateCountWindowConfig(Long count, Long slideCount, WindowType type) {
        if (count == null || count <= 0)
            throw new IllegalArgumentException("Window count must be > 0");
        if (slideCount == null)
            throw new IllegalArgumentException("Slide count must be specified (use 0 for tumbling)");
        if (slideCount < 0)
            throw new IllegalArgumentException("Slide count cannot be negative");
        if (slideCount > count)
            throw new IllegalArgumentException(String.format(
                    "Slide count (%d) cannot exceed window count (%d) — this would create gaps",
                    slideCount, count));
    }

    /**
     * Window assigner für NetworkPacket (Buffered Time-Pipeline).
     */
    private static WindowAssigner<? super NetworkPacket, TimeWindow> createTimeWindowAssigner(
            WindowType type, long size, Long slide) {
        switch (type) {
            case TUMBLING_WINDOW: return TumblingEventTimeWindows.of(Duration.ofMillis(size));
            case SLIDING_WINDOW:  return SlidingEventTimeWindows.of(
                    Duration.ofMillis(size), Duration.ofMillis(slide));
            case SESSION_WINDOW:  return EventTimeSessionWindows.withGap(Duration.ofMillis(size));
            default:              return TumblingProcessingTimeWindows.of(Duration.ofMillis(size));
        }
    }

    /**
     * Window assigner für PacketFeatures (Incremental Time-Pipeline).
     * Identische Konfiguration — nur der Typ-Parameter unterscheidet sich.
     */
    private static WindowAssigner<? super PacketFeatures, TimeWindow> createFeatureWindowAssigner(
            WindowType type, long size, Long slide) {
        switch (type) {
            case TUMBLING_WINDOW: return TumblingEventTimeWindows.of(Duration.ofMillis(size));
            case SLIDING_WINDOW:  return SlidingEventTimeWindows.of(
                    Duration.ofMillis(size), Duration.ofMillis(slide));
            case SESSION_WINDOW:  return EventTimeSessionWindows.withGap(Duration.ofMillis(size));
            default:              return TumblingProcessingTimeWindows.of(Duration.ofMillis(size));
        }
    }


    /**
     * Loads a DetectionAlgorithm from a JAR file.
     * Used both on the client side (for validation) and on TaskManagers
     * (with the local path from the Distributed Cache).
     */
    public static DetectionAlgorithm loadAlgorithmFromJar(String className, String jarPath) throws Exception {
        File jarFile = new File(jarPath);
        if (!jarFile.exists()) {
            throw new IllegalArgumentException("Algorithm JAR not found: " + jarPath);
        }

        long currentModified = jarFile.lastModified();

        // Check if cached ClassLoader is still valid (JAR unchanged)
        CachedClassLoader cached = classLoaderCache.get(jarPath);
        if (cached != null && cached.jarLastModified != currentModified) {
            LOG.info("JAR file changed on disk, evicting stale ClassLoader for: {}", jarPath);
            evictClassLoader(jarPath);
            cached = null;
        }

        // Create or reuse ClassLoader
        if (cached == null) {
            URLClassLoader newLoader = new URLClassLoader(
                    new URL[]{jarFile.toURI().toURL()},
                    AlgorithmJobFactory.class.getClassLoader()
            );
            cached = new CachedClassLoader(newLoader, currentModified);
            CachedClassLoader existing = classLoaderCache.putIfAbsent(jarPath, cached);
            if (existing != null) {
                try { newLoader.close(); } catch (Exception ignored) {}
                cached = existing;
            } else {
                LOG.info("Created new ClassLoader for JAR: {}", jarPath);
            }
        }

        Class<?> clazz = Class.forName(className, true, cached.classLoader);
        if (!DetectionAlgorithm.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("Class " + className + " does not implement DetectionAlgorithm");
        }
        return (DetectionAlgorithm) clazz.getDeclaredConstructor().newInstance();
    }

    public static void evictClassLoader(String jarPath) {
        CachedClassLoader removed = classLoaderCache.remove(jarPath);
        if (removed != null) {
            try {
                removed.classLoader.close();
                LOG.info("Evicted and closed ClassLoader for: {}", jarPath);
            } catch (Exception e) {
                LOG.warn("Failed to close evicted ClassLoader for {}: {}", jarPath, e.getMessage());
            }
        }
    }

    public static void closeAllClassLoaders() {
        LOG.info("Closing all cached ClassLoaders (count: {})", classLoaderCache.size());
        for (String path : classLoaderCache.keySet()) {
            evictClassLoader(path);
        }
    }

    /**
     * MapFunction that extracts a key from a NetworkPacket using a PacketKeyExtractor.
     */
    private static class KeyExtractorMapFunction
            extends RichMapFunction<NetworkPacket, NetworkPacket> {

        private static final long serialVersionUID = 1L;
        private final String algorithmClassName;
        private transient PacketKeyExtractor extractor;

        KeyExtractorMapFunction(String algorithmClassName) {
            this.algorithmClassName = algorithmClassName;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            File jarFile = getRuntimeContext().getDistributedCache()
                    .getFile(ALGORITHM_JAR_CACHE_KEY);
            this.extractor = loadAlgorithmFromJar(algorithmClassName, jarFile.getAbsolutePath()).getKeyExtractor();
        }

        @Override
        public NetworkPacket map(NetworkPacket packet) throws Exception {
            packet.setCustomKey(extractor.getKey(packet));
            // Kryo überträgt nur rawPacketData (~54 bytes) statt Pcap4j-Objekt (~50KB)
            //  m Window wird packetCache bei Bedarf aus rawPacketData neu aufgebaut
            packet.setPacketCache(null);
            return packet;
        }
    }

}
