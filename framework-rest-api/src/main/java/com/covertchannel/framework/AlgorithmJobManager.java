package com.covertchannel.framework;


import com.covertchannel.framework.api.JobContext;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AlgorithmJobManager
 * 
 * Verwaltet den Lifecycle von Detektionsalgorithmus Jobs.
 * Submittet, stoppt und überwacht Flink Jobs.
 * 
 * Window handling
 * Der Manager leitet Window-Konfiguration aus AlgorithmJobSubmission
 * direkt an AlgorithmJobFactory weiter. Dies trennt Concerns:
 *  REST-API: empfängt Config
 * JobManager: orchestriert Submission
 * JobFactory: entscheidet konkrete Window Implementierung
 */

public class AlgorithmJobManager {

    private static final Logger LOG = LoggerFactory.getLogger(AlgorithmJobManager.class);


    private RestClusterClient<String> flinkClusterClient;


    public AlgorithmJobManager(RestClusterClient<String> flinkClusterClient) {
        this.flinkClusterClient = flinkClusterClient;
    }

    /**
     * Submittet  Detektionsalgorithmus Job
     *
     * Der Flow
     * 1. Erzeugt  JobContext aus Submission Map
     * 2. Nutzt die Factory für JobGraphs aus Context
     * 3. Registriert den Job in der lokalen Registry
     */
    public String submitAlgorithmJob(AlgorithmJobSubmission submission) throws Exception {
        LOG.info("Submitting algorithm job: {}", submission.getAlgorithmId());

        // Erzeuge den neuen JobContext (Parameter Object)
        JobContext context = new JobContext(submission.getConfiguration());

        LOG.info("  Algorithm: {}", context.getAlgorithmClassName());
        LOG.info("  JAR: {}", submission.getJarPath());
        LOG.info("  Kafka: brokers={}, input={}, output={}",
                context.getKafkaBrokers(), context.getParam("inputTopic"), context.getParam("outputTopic"));

        try {

            // 1 Erstelle JobGraph

            JobGraph jobGraph = AlgorithmJobFactory.createJobGraph(
                            submission.getJarPath(),
                            context)
                    .getJobGraph();

            //2 Addiere JAR zu JobGraph für Verteilung

            //LOG.info("Adding algorithm JAR to JobGraph for distribution");

            String jarPath = submission.getJarPath();
            File jarFile = new File(jarPath);
            jobGraph.addJar(new org.apache.flink.core.fs.Path(jarFile.toURI()));


            // 3 Submittet zu Flink Cluster

            LOG.info("Submitting JobGraph to Flink cluster for algorithm: {}", submission.getAlgorithmId());
            JobID jobID = flinkClusterClient.submitJob(jobGraph).get();
            String jobIdString = jobID.toString();

            LOG.info("Job successfully registered with ID: {}", jobIdString);

            return jobIdString;

        } catch (Exception e) {
            LOG.error("Failed to submit algorithm job: {}", submission.getAlgorithmId(), e);
            throw new Exception("Job submission failed: " + e.getMessage(), e);
        }
    }

}

