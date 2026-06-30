package com.covertchannel.framework.rest;

import com.covertchannel.framework.AlgorithmJobManager;
import com.covertchannel.framework.AlgorithmJobSubmission;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * REST API Controller für Submission und Management
 * mit Ordnerstruktur und einzelnen JAR/Config Uploads
 * 
 * Kafka Topics werden aus der config.json gelesen!
 */
@RestController
@RequestMapping("/api/algorithms")
public class AlgorithmSubmissionRestApi {
    private static final Logger LOG = LoggerFactory.getLogger(AlgorithmSubmissionRestApi.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Autowired
    private AlgorithmJobManager jobManager;


    @Autowired
    private AlgorithmJarManager algorithmJarManager;

    @Value("${results-storage-dir:/opt/flink/results}")
    private String resultsStorageDir;


    /**
     * Parst Config und erstellt JobSubmission
     * 
     * @param algorithmId Die Algorithmus-ID
     * @param jarPath Der Pfad zur JAR
     * @return AlgorithmJobSubmission mit allen Werten aus config.json
     * @throws Exception wenn Config fehlt oder ungültig ist
     */
    private AlgorithmJobSubmission parseConfigAndCreateSubmission(String algorithmId, String jarPath) throws Exception {
        // Lese Config
        String configJson = algorithmJarManager.readConfig(algorithmId);
        
        if (configJson == null || configJson.isEmpty()) {
            throw new IllegalArgumentException("Config nicht gefunden für: " + algorithmId);
        }
        // Parse als Map
        Map<String, Object> configMap = MAPPER.readValue(configJson, Map.class);

        return new AlgorithmJobSubmission(algorithmId, jarPath, configMap);

    }

    /**
     * Holt String aus Config validiert
     */
    private String getRequiredField(Map<String, Object> configMap, String fieldName) throws IllegalArgumentException {
        Object value = configMap.get(fieldName);
        if (value == null || value.toString().trim().isEmpty()) {
            throw new IllegalArgumentException("Erforderliches Feld fehlt in config.json: " + fieldName);
        }
        return value.toString();
    }

    /**
     * Holt  String mit Default
     */
    private String getOptionalStringField(Map<String, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        if (value == null) return defaultValue;
        String strValue = value.toString().trim();
        if (strValue.isEmpty()) return defaultValue;
        return strValue;
    }

    /**
     * Holt Long mit Default
     */
    private long getOptionalLongField(Map<String, Object> configMap, String fieldName, long defaultValue) {
        Object value = configMap.get(fieldName);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            LOG.warn("Invalid {} value: {}, using default: {}", fieldName, value, defaultValue);
            return defaultValue;
        }
    }


    private Path getResultsBasePath() {
        return Paths.get(resultsStorageDir).toAbsolutePath().normalize();
    }

    private Path resolveResultPath(String relativePath) {
        Path basePath = getResultsBasePath();
        Path resolvedPath = basePath.resolve(relativePath).normalize();

        if (!resolvedPath.startsWith(basePath)) {
            throw new IllegalArgumentException("Ungültiger Pfad");
        }

        return resolvedPath;
    }

    private Map<String, Object> buildResultNode(Path path, Path basePath) throws IOException {
        Map<String, Object> node = new LinkedHashMap<>();
        boolean directory = Files.isDirectory(path);
        Path relativePath = basePath.equals(path) ? Paths.get("") : basePath.relativize(path);

        node.put("name", path.getFileName() != null ? path.getFileName().toString() : basePath.getFileName().toString());
        node.put("path", relativePath.toString().replace("\\", "/"));
        node.put("directory", directory);

        if (directory) {
            List<Map<String, Object>> children = new ArrayList<>();
            try (var stream = Files.list(path)) {
                stream.sorted(Comparator.comparing(p -> p.getFileName().toString().toLowerCase()))
                        .forEach(child -> {
                            try {
                                children.add(buildResultNode(child, basePath));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
            node.put("children", children);
        } else {
            node.put("size", Files.size(path));
        }

        return node;
    }


    /**
     * Hochladen  JAR
     * POST /api/algorithms/upload-jar/cabuk-v1
     */
    @PostMapping("/upload-jar/{algorithmId}")
    public ResponseEntity<?> uploadJar(
            @PathVariable String algorithmId,
            @RequestParam("jar") MultipartFile jarFile) {
        LOG.info("Received JAR upload for algorithm: {}", algorithmId);

        try {
            if (jarFile.isEmpty()) {
                throw new IllegalArgumentException("JAR file is empty");
            }

            // Speichere JAR in Ordner: /opt/flink-plugins/{algorithmId}/
            String jarFileName = algorithmJarManager.saveJarToAlgorithm(
                    algorithmId,
                    jarFile.getOriginalFilename(),
                    jarFile.getBytes()
            );

            LOG.info("JAR uploaded successfully: {}", jarFileName);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("algorithmId", algorithmId);
            response.put("jarFileName", jarFileName);
            response.put("message", "JAR hochgeladen");

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            LOG.error("Failed to upload JAR", e);
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * Hochladen  Config
     * POST /api/algorithms/upload-config/cabuk-v1
     */
    @PostMapping("/upload-config/{algorithmId}")
    public ResponseEntity<?> uploadConfig(
            @PathVariable String algorithmId,
            @RequestParam("config") MultipartFile configFile) {
        LOG.info("Received config upload for algorithm: {}", algorithmId);

        try {
            if (configFile.isEmpty()) {
                throw new IllegalArgumentException("Config file is empty");
            }

            // Validiere JSON
            String configJson = new String(configFile.getBytes());
            MAPPER.readValue(configJson, Map.class);

            // Speichere Config in Ordner: /opt/flink-plugins/{algorithmId}/config.json
            algorithmJarManager.saveConfigToAlgorithm(algorithmId, configJson);

            LOG.info("Config uploaded successfully for algorithm: {}", algorithmId);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("algorithmId", algorithmId);
            response.put("message", "Config hochgeladen");

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            LOG.error("Failed to upload config", e);
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }



    /**
     * Listet JAR/Config Status
     * GET /api/algorithms/list
     */
    @GetMapping("/list")
    public ResponseEntity<?> listAlgorithms() {
        LOG.debug("Listing all algorithms");

        try {
            List<Map<String, Object>> algorithms = algorithmJarManager.listAlgorithmsWithStatus();
            
            Map<String, Object> response = new HashMap<>();
            response.put("algorithms", algorithms);
            response.put("count", algorithms.size());

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            LOG.error("Failed to list algorithms", e);
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * Listet Results
     * GET /api/algorithms/results/tree
     */
    @GetMapping("/results/tree")
    public ResponseEntity<?> getResultsTree() {
        try {
            Path basePath = getResultsBasePath();

            if (!Files.exists(basePath)) {
                Map<String, Object> response = new LinkedHashMap<>();
                response.put("name", basePath.getFileName().toString());
                response.put("path", "");
                response.put("directory", true);
                response.put("children", Collections.emptyList());
                return ResponseEntity.ok(response);
            }

            return ResponseEntity.ok(buildResultNode(basePath, basePath));
        } catch (Exception e) {
            LOG.error("Failed to read results tree", e);
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * Lädt  Datei aus dem Results
     * GET /api/algorithms/results/download?path=subdir/file.jsonl
     */
    @GetMapping("/results/download")
    public ResponseEntity<?> downloadResultFile(@RequestParam("path") String relativePath) {
        try {
            Path filePath = resolveResultPath(relativePath);

            if (!Files.exists(filePath) || Files.isDirectory(filePath)) {
                throw new IllegalArgumentException("Datei nicht gefunden");
            }

            InputStreamResource resource = new InputStreamResource(Files.newInputStream(filePath));

            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filePath.getFileName() + "\"")
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .contentLength(Files.size(filePath))
                    .body(resource);
        } catch (Exception e) {
            LOG.error("Failed to download result file: {}", relativePath, e);
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * Löscht Ordner JAR  Config
     * DELETE /api/algorithms/{algorithmId}
     */
    @DeleteMapping("/{algorithmId}")
    public ResponseEntity<?> deleteAlgorithm(@PathVariable String algorithmId) {
        LOG.info("Deleting algorithm: {}", algorithmId);

        try {
            algorithmJarManager.deleteAlgorithm(algorithmId);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Algorithmus gelöscht: " + algorithmId);

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            LOG.error("Failed to delete algorithm", e);
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * Löscht JAR
     * DELETE /api/algorithms/{algorithmId}/jar
     */
    @DeleteMapping("/{algorithmId}/jar")
    public ResponseEntity<?> deleteJar(@PathVariable String algorithmId) {
        LOG.info("Deleting JAR for algorithm: {}", algorithmId);

        try {
            algorithmJarManager.deleteJarFromAlgorithm(algorithmId);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "JAR gelöscht");

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            LOG.error("Failed to delete JAR", e);
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }


    /**
     * Submittet zur Ausführung
     * POST /api/algorithms/{algorithmId}/execute
     *
     */
    @PostMapping("/{algorithmId}/execute")
    public ResponseEntity<?> executeAlgorithm(@PathVariable String algorithmId) {
        
        LOG.info("Executing algorithm: {}", algorithmId);

        try {
            // Hole Pfad
            String jarPath = algorithmJarManager.getJarPathForAlgorithm(algorithmId);
            if (jarPath == null || jarPath.isEmpty()) {
                throw new IllegalArgumentException("JAR nicht gefunden für: " + algorithmId);
            }

            // Parse Config, erstelle Submission
            AlgorithmJobSubmission submission = parseConfigAndCreateSubmission(algorithmId, jarPath);

            // Submitte Job
            String jobId = jobManager.submitAlgorithmJob(submission);
            LOG.info("✓ Job submitted with ID: {}", jobId);

            Map<String, Object> config = submission.getConfiguration();

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("jobId", jobId);
            response.put("algorithmId", algorithmId);
            response.put("message", "Algorithmus ausgeführt");
            response.put("kafkaBrokers", config.get("kafkaBrokers"));
            response.put("inputTopic", config.get("inputTopic"));
            response.put("outputTopic", config.get("outputTopic"));

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            LOG.error("Failed to execute algorithm", e);
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }
}
