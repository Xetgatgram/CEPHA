package com.covertchannel.framework.rest;

import com.covertchannel.framework.AlgorithmJobFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service für Verwaltung ordnern mit JAR, Config
 * Ordnerstruktur  /opt/flink-plugins/{algorithmId}/
 *                                      ├── {name}.jar
 *                                      └── config.json
 */
@Service
public class AlgorithmJarManager {
    private static final Logger LOG = LoggerFactory.getLogger(AlgorithmJarManager.class);
    private final String PLUGINS_STORAGE_DIR;

    public AlgorithmJarManager( @Value("${plugin-storage-dir}") String PLUGINS_STORAGE_DIR) {
        // Erstelle Storageverzeichnis falls nicht vorhanden
       this.PLUGINS_STORAGE_DIR = PLUGINS_STORAGE_DIR;
        File storageDir = new File(PLUGINS_STORAGE_DIR);
        if (!storageDir.exists()) {
            if (storageDir.mkdirs()) {
                LOG.info("Created plugins storage directory: {}", PLUGINS_STORAGE_DIR);
            }
        }
    }

    /**
     * Speichert  JAR in ordner
     * Pfad: /opt/flink-plugins/{algorithmId}/{jarFileName}
     */
    public String saveJarToAlgorithm(String algorithmId, String jarFileName, byte[] jarData) throws IOException {
        validateAlgorithmId(algorithmId);
        
        // Erstelle Ordner für Algorithmus
        String algorithmDir = PLUGINS_STORAGE_DIR + "/" + algorithmId;
        File dir = new File(algorithmDir);
        if (!dir.exists()) {
            dir.mkdirs();
            LOG.info("Created algorithm directory: {}", algorithmDir);
        }

        // Speichere JAR
        String filePath = algorithmDir + "/" + sanitizeFileName(jarFileName);
        AlgorithmJobFactory.evictClassLoader(filePath);
        Files.write(Paths.get(filePath), jarData);
        
        LOG.info("JAR saved for algorithm {}: {}", algorithmId, filePath);
        return filePath;
    }

    /**
     * Speichert Config datei ,JSON für Algorithmus
     * Pfad: /opt/flink-plugins/{algorithmId}/config.json
     */
    public void saveConfigToAlgorithm(String algorithmId, String configJson) throws IOException {
        validateAlgorithmId(algorithmId);
        
        // Erstelle Ordner falls nicht vorhanden
        String algorithmDir = PLUGINS_STORAGE_DIR + "/" + algorithmId;
        File dir = new File(algorithmDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        // Speichere Config als config.json
        String configPath = algorithmDir + "/config.json";
        try (FileWriter writer = new FileWriter(configPath, StandardCharsets.UTF_8)) {
            writer.write(configJson);
        }
        
        LOG.info("Config saved for algorithm {}: {}", algorithmId, configPath);
    }

    /**
     * Liest die Config
     */
    public String readConfig(String algorithmId) throws IOException {
        String configPath = PLUGINS_STORAGE_DIR + "/" + algorithmId + "/config.json";
        Path path = Paths.get(configPath);
        
        if (!Files.exists(path)) {
            return null;
        }
        
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    /**
     * Holt den Pfad der JAR
     */
    public String getJarPathForAlgorithm(String algorithmId) {
        String algorithmDir = PLUGINS_STORAGE_DIR + "/" + algorithmId;
        File dir = new File(algorithmDir);
        
        if (!dir.exists()) {
            return null;
        }
        
        File[] jars = dir.listFiles((d, name) -> name.endsWith(".jar"));
        if (jars != null && jars.length > 0) {
            return jars[0].getAbsolutePath();
        }
        
        return null;
    }

    /**
     * Listet alle Algorithmen  auf
     * Status:  algorithmId, hasJar, hasConfig
     */
    public List<Map<String, Object>> listAlgorithmsWithStatus() {
        List<Map<String, Object>> algorithms = new ArrayList<>();
        File storageDir = new File(PLUGINS_STORAGE_DIR);
        
        if (!storageDir.exists()) {
            return algorithms;
        }

        File[] algorithmDirs = storageDir.listFiles(File::isDirectory);
        if (algorithmDirs == null) {
            return algorithms;
        }

        for (File dir : algorithmDirs) {
            String algorithmId = dir.getName();
            
            // Prüfe auf JAR und Config
            File[] jars = dir.listFiles((d, name) -> name.endsWith(".jar"));
            File configFile = new File(dir, "config.json");

            Map<String, Object> algInfo = new HashMap<>();
            algInfo.put("algorithmId", algorithmId);
            algInfo.put("hasJar", jars != null && jars.length > 0);
            algInfo.put("jarFile", jars != null && jars.length > 0 ? jars[0].getName() : null);
            algInfo.put("hasConfig", configFile.exists());
            algInfo.put("configFile", configFile.exists() ? "config.json" : null);
            algInfo.put("ready", (jars != null && jars.length > 0) && configFile.exists());
            algInfo.put("path", dir.getAbsolutePath());

            algorithms.add(algInfo);
        }

        return algorithms;
    }

    /**
     * Löscht Ordner JAR + Config
     */
    public void deleteAlgorithm(String algorithmId) throws IOException {
        validateAlgorithmId(algorithmId);

        evictClassLoadersForAlgorithm(algorithmId);

        String algorithmDir = PLUGINS_STORAGE_DIR + "/" + algorithmId;
        Path path = Paths.get(algorithmDir);
        
        if (Files.exists(path)) {
            // Lösche alle Dateien im Ordner
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            LOG.warn("Failed to delete: {}", p, e);
                        }
                    });
            LOG.info("Algorithm deleted: {}", algorithmId);
        }
    }

    /**
     * Löscht nur die JAR
     */
    public void deleteJarFromAlgorithm(String algorithmId) throws IOException {
        String algorithmDir = PLUGINS_STORAGE_DIR + "/" + algorithmId;
        File dir = new File(algorithmDir);
        
        if (!dir.exists()) {
            throw new IOException("Algorithm directory not found: " + algorithmId);
        }

        File[] jars = dir.listFiles((d, name) -> name.endsWith(".jar"));
        if (jars != null) {
            for (File jar : jars) {
                AlgorithmJobFactory.evictClassLoader(jar.getAbsolutePath());
                Files.delete(jar.toPath());
                LOG.info("JAR deleted: {}", jar.getName());
            }
        }
    }

    /**
     * Evicts cached ClassLoaders
     */
    private void evictClassLoadersForAlgorithm(String algorithmId) {
        String algorithmDir = PLUGINS_STORAGE_DIR + "/" + algorithmId;
        File dir = new File(algorithmDir);
        if (dir.exists()) {
            File[] jars = dir.listFiles((d, name) -> name.endsWith(".jar"));
            if (jars != null) {
                for (File jar : jars) {
                    AlgorithmJobFactory.evictClassLoader(jar.getAbsolutePath());
                }
            }
        }
    }

    /**
     * Validiert Algorithm ID
     */
    private void validateAlgorithmId(String algorithmId) throws IllegalArgumentException {
        if (algorithmId == null || !algorithmId.matches("[a-zA-Z0-9-_]+")) {
            throw new IllegalArgumentException("Invalid algorithm ID: " + algorithmId);
        }
    }

    /**
     * Sanitized Path Traversal
     */
    private String sanitizeFileName(String fileName) {
        return fileName.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    /**
     * Gibt den Storage aus
     */
    public String getStorageDirectory() {
        return PLUGINS_STORAGE_DIR;
    }
}
