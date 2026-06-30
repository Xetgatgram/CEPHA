package com.covertchannel.producer;

//import com.covertchannel.reader.PcapFileReader;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.Pcaps;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.namednumber.DataLinkType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Example application that reads PCAP file and sends packets to Kafka.
 *
 * <p>Usage:</p>
 * <pre>
 *   java PcapToKafkaApplication &lt;pcap-file&gt; &lt;kafka-brokers&gt; &lt;kafka-topic&gt;
 * </pre>
 *
 * <p>Example:</p>
 * <pre>
 *   java PcapToKafkaApplication traffic.pcap localhost:9092 network-packets
 * </pre>
 */
public class PcapToKafkaApplication {

    private static final Logger LOG = LoggerFactory.getLogger(PcapToKafkaApplication.class);

    public static void main(String[] args) {
        if (args.length < 3) {
            printUsage();
            System.exit(1);
        }

        String sourcePath = args[0];
        String kafkaBrokers = args[1];
        String kafkaTopic = args[2];
        int maxPackets = args.length > 3 ? Integer.parseInt(args[3]) : 0;

        List<File> pcapFiles = getFilesToProcess(sourcePath);

        if (pcapFiles.isEmpty()) {
            LOG.error("No PCAP files found at: {}", sourcePath);
            System.exit(1);
        }

        LOG.info("Starting PcapToKafkaApplication");
        LOG.info("  Source: {}", sourcePath);
        LOG.info("  Files found: {}", pcapFiles.size());
        LOG.info("  Kafka Brokers: {}", kafkaBrokers);
        LOG.info("  Kafka Topic: {}", kafkaTopic);

        KafkaPacketProducer kafkaProducer = new KafkaPacketProducer(kafkaBrokers, kafkaTopic);

        try {
            long totalDuration = 0;
            long totalPackets = 0;

            for (File file : pcapFiles) {
                LOG.info("Processing file: {}", file.getName());
                
                long startTime = System.currentTimeMillis();
                
                // Read PCAP and send to Kafka
                // Note: We might want to pass remaining maxPackets if the limit is global
                int limitForThisFile = maxPackets == 0 ? 0 : (maxPackets - (int)totalPackets);
                if (maxPackets > 0 && limitForThisFile <= 0) break;

                PcapFileReader reader = new PcapFileReader(file.getAbsolutePath(), kafkaProducer, limitForThisFile);
                PcapFileReader.ProcessingResult result = reader.process();

                long duration = System.currentTimeMillis() - startTime;
                totalDuration += duration;
                
                // Access statistics from reader if available, otherwise just log completion
                LOG.info("Finished file: {} in {} ms", file.getName(), duration);
            }

            // Print global statistics
            LOG.info("TOTAL STATISTICS");
            LOG.info("Duration: {} ms", totalDuration);
            LOG.info("Kafka Producer Stats: {}", kafkaProducer.getStatistics());


        } catch (Exception e) {
            LOG.error("Error during processing: {}", e.getMessage(), e);
        } finally {
            kafkaProducer.close();
        }

        LOG.info("Application finished");
    }

    private static List<File> getFilesToProcess(String path) {
        List<File> files = new ArrayList<>();
        File source = new File(path);

        if (source.isFile()) {
            files.add(source);
        } else if (source.isDirectory()) {
            File[] fileList = source.listFiles((dir, name) -> name.endsWith(".pcap") || name.endsWith(".pcapng"));
            if (fileList != null) {
                files.addAll(Arrays.asList(fileList));
            }
        }
        return files;
    }

    private static void printUsage() {
        System.out.println("Usage: PcapToKafkaApplication <file-or-directory> <kafka-brokers> <kafka-topic> [max-packets]");
    }

}
