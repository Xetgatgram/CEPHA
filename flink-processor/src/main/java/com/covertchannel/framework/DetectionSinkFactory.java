package com.covertchannel.framework;

import com.covertchannel.processor.DetectionResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.io.IOException;
import java.time.Duration;

/**
 * Factory for creating Output Sinks.
 * Encapsulates the complexity of File I/O, Rolling Policies, and Serialization.
 *
 * *Rolling Policy Strategy:
 *     BATCH      DefaultRollingPolicy (size/time/inactivity based — checkpoints finalize via BATCH semantics)
 *     STREAMING  OnCheckpointRollingPolicy (finalisiert Part-Dateien bei jedem Checkpoint)
 *     REPLAY     OnCheckpointRollingPolicy (bounded stream → letzter Checkpoint am Job-Ende finalisiert alles)
 *
 *   Hintergrund: FileSink schreibt Daten zunächst als .inprogress-Dateien.
 *   Ohne Checkpoint-basierte Finalisierung (STREAMING/REPLAY) bleiben diese Dateien
 *   unvollständig liegen. OnCheckpointRollingPolicy stellt sicher, dass bei jedem
 *  Checkpoint (inkl. dem finalen am Job-Ende) alle offenen Parts zu .jsonl finalisiert werden.
 */
public class DetectionSinkFactory {

    private static final ObjectMapper MAPPER = buildMapper();

    public static FileSink<DetectionResult> createJsonlSink(String algorithmName, String execMode) {
        return FileSink
                .forRowFormat(resolvePath(algorithmName), DetectionSinkFactory::serialize)
                .withRollingPolicy(buildRollingPolicy(execMode))
                .build();
    }

    // Path

    private static Path resolvePath(String algorithmName) {
        String base = requireEnv(CephaConfig.ENV_OUTPUT_PATH);
        String normalized = base.endsWith("/") ? base : base + "/";
        return new Path(normalized + algorithmName);
    }

    // Serialization

    private static ObjectMapper buildMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.getFactory().disable(
                com.fasterxml.jackson.core.JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        return mapper;
    }

    private static void serialize(DetectionResult element, java.io.OutputStream stream)
            throws IOException {
        try {
            stream.write(MAPPER.writeValueAsBytes(element));
            stream.write('\n');
        } catch (JsonProcessingException e) {
            throw new IOException("Failed to serialize DetectionResult", e);
        }
    }


    // Rolling Policy

    private static RollingPolicy<DetectionResult, String> buildRollingPolicy(String execMode) {
        if ("BATCH".equalsIgnoreCase(execMode)) {
            return DefaultRollingPolicy.<DetectionResult, String>builder()
                    .withRolloverInterval(Duration.ofSeconds(
                            readLongEnv(CephaConfig.ENV_OUTPUT_ROLLOVER_INTERVAL,
                                    CephaConfig.DEFAULT_ROLLOVER_SEC)))
                    .withInactivityInterval(Duration.ofSeconds(
                            readLongEnv(CephaConfig.ENV_OUTPUT_INACTIVITY_INTERVAL,
                                    CephaConfig.DEFAULT_INACTIVITY_SEC)))
                    .withMaxPartSize(
                            readLongEnv(CephaConfig.ENV_OUTPUT_MAX_SIZE_MB,
                                    CephaConfig.DEFAULT_MAX_SIZE_MB) * 1024L * 1024L)
                    .build();
        }
        // STREAMING + REPLAY: Checkpoint-basierte Finalisierung
        return OnCheckpointRollingPolicy.build();
    }


    // Env helpers


    private static String requireEnv(String key) {
        String val = System.getenv(key);
        if (val == null || val.isBlank()) {
            throw new IllegalStateException(
                    "Environment variable '" + key + "' is not set. "
                            + "Configure " + key + " in your Docker Compose file.");
        }
        return val;
    }

    private static long readLongEnv(String key, long defaultValue) {
        String val = System.getenv(key);
        return val != null ? Long.parseLong(val) : defaultValue;
    }
}