package com.covertchannel.framework.rest;


import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.covertchannel.framework.AlgorithmJobManager;

/**
 * Spring Configuration für Flink Integration.
 * Erstellt Beans für Flink RestClusterClient und AlgorithmJobManager.
 */
@Component
public class FlinkConfiguration {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkConfiguration.class);
    
    @Value("${flink.cluster.host:jobmanager}")
    private String flinkClusterHost;

    
    @Value("${flink.cluster.port:6123}")
    private Integer flinkClusterPort;
    
    /**
     * Erstellt Flink RestClusterClient Bean.
     */
    @Bean
    public RestClusterClient<String> flinkRestClusterClient() throws Exception {
        LOG.info("Connecting to Flink cluster at {}:{}", flinkClusterHost, flinkClusterPort);
        
        Configuration config = new Configuration();
        config.setString("jobmanager.rpc.address", flinkClusterHost);
        config.setInteger("jobmanager.rpc.port", flinkClusterPort);
        
        RestClusterClient<String> client = new RestClusterClient<String>(config, "default");
        LOG.info("✓ Connected to Flink cluster");
        
        return client;
    }
    
    /**
     * Erstellt AlgorithmJobManager Bean.
     */
    @Bean
    public AlgorithmJobManager algorithmJobManager(RestClusterClient<String> restClusterClient) {
        LOG.info("Creating AlgorithmJobManager bean");
        return new AlgorithmJobManager(restClusterClient);
    }
}
