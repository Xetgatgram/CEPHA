# CEPHA Technical Reference Documentation

---

## Chapter 1 Introduction

### 1.1 Purpose of this Document

This document is the technical reference for **CEPHA** (Covert channel Examination, Packet based Hidden channel Analysis), a modular, plugin framework that supports dynamic JAR deployment for network covert-channel detection algorithms. It describes the architecture, data model, module structure, plugin system, and configuration of the framework in technical detail.

This document is one of three documentations, developed as part of a Master's thesis in Computer Science at FernUniversität Hagen:

- **Technical Reference:** Architecture, data model, module internals, plugin contract, and configuration reference.
- **Deployment Guide**: Instructions for running CEPHA locally or in a distributed cloud environment.
- **Plugin Development Guide**: Conceptual overview of the plugin architecture and contract, project setup, windowing strategy reference, and a step-by-step implementation walkthrough.

Each document is self-contained. Readers who only need to deploy or extend the framework may consult the Deployment Guide or Plugin Development Tutorial directly.

### 1.2 Target Audience & Prerequisites

This document is written for developers and researchers who want to understand, extend, or contribute to CEPHA. The following background knowledge is assumed:

- **Java** (intermediate level generics, interfaces, serialization)
- **Apache Flink** (data pipelines, windowing, watermarks)
- **Apache Kafka** (topics, producers , consumers)
- **Maven** (multi module builds, dependency scopes)

The following knowledge is helpful but not required:

- **Docker & Docker Compose** (basic usage)
- **Spring Boot** (application configuration, REST controllers)


### 1.3 Technology Stack

CEPHA is built on the following technologies:

| Component                | Technology   | Version |
| ------------------------ | ------------ | ------- |
| Data Processing          | Apache Flink | 1.20.3  |
| Message Broker           | Apache Kafka | 3.6.1   |
| REST API Framework       | Spring Boot  | 3.1.5   |
| Packet Capture / Parsing | Pcap4j       | 1.8.2   |

---

## Chapter 2 Architecture Overview

### 2.1 Module Overview and Dependencies

CEPHA is structured as a Maven multi-module project. Each module has a clearly defined responsibility. The modules communicate through two channels: a Kafka topic between `kafka-producer` and `flink-processor`, and a REST interface exposed by `framework-rest-api`.

| Module               | Responsibility                                                                                                                                                                                                                         | Depends on                                                                                               |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `detection-api`      | Defines the public plugin contract: the `DetectionAlgorithm` interface, shared data types, and DTOs. The sole required dependency for plugin development.                                                                              | `pcap4j-core`                                                                                            |
| `kafka-producer`     | Captures network packets from PCAP files or live network interfaces and publishes binary records to a Kafka topic. It is embedded in `framework-rest-api`.  Can also be operated standalone through CLI.                               | `pcap4j-core`, `kafka-clients`                                                                           |
| `flink-processor`    | Consumes binary records from Kafka, applies windowing, dynamically loads and invokes plugin algorithms, and writes detection results to the filesystem. Compiled as a library; invoked by `framework-rest-api` at job submission time. | `detection-api`, `flink-streaming-java`, `flink-connector-kafka`, `flink-connector-files`, `pcap4j-core` |
| `framework-rest-api` | Spring Boot application. Exposes REST endpoints for plugin and job management, Kafka administration, and producer control. Embeds `kafka-producer` for in-process packet capture and submits Flink jobs via `flink-processor`.         | `detection-api`, `flink-processor`, `kafka-producer`, `spring-boot`, `flink-clients`                     |

In addition to the four core Maven modules, the repository contains independently built plugin projects such as cabuk-detector and port-entropy-detector. These plugins are distributed with CEPHA as reference implementations, but they are not part of the parent multi-module build.



### 2.2 System Diagram


![[systemdiagramm.png|462]]

#### Dependencies

![[dependencies.png|697]]




## Chapter 3 Data Model & Data Flow

### 3.1 Dataflow

The processing pipeline begins at the packet capture stage. The `kafka-producer` reads packets from a `.pcap` file via `PcapFileReader` or captures them from a live network interface, and publishes each packet as a binary record to a Kafka topic. The `flink-processor` consumes these records, deserializes each into a `NetworkPacket` object (see Section 3.3), and partitions the resulting stream into keyed flows using `keyBy()`. The key is derived from each packet by a `PacketKeyExtractor` and defines flow identity. Depending on the job configuration, one of three pipeline modes is applied.

- **Time-based window:** Packets are accumulated over a fixed time interval or a session window separated by an inactivity gap.
    
- **Count-based window:** Packets are accumulated until a configured number, per flow key, is reached.  
    
- **Per-packet (no window):** Each packet is processed individually without accumulation. No state is managed by the framework. 
    

In windowed modes, `processFlow()` is called for every `NetworkPacket` within a window,, `detect()` is called once when the window closes. In per-packet mode, `detect()` is called immediately after each `processFlow()` invocation. The framework drives this sequencing, the plugin does not distinguish between pipeline modes.
The `DetectionResult` is written to the filesystem as JSONL via a `FileSink`.


![[DataFlow.png|697]]
## 3.2 Binary Record Format

Each message written to Kafka by the `kafka-producer` consists of a fixed 12-byte header followed by the raw packet bytes. 
The header contains the Data Link Type and the capture timestamp. The `dlt` is required by `flink-processor` to reconstruct a typed Pcap4j `Packet` object from the raw bytes.

`[[int dlt][long timestamp][byte[] rawPacketData]]`

| Field           | Type     | Size     | Description                                                              |
| --------------- | -------- | -------- | ------------------------------------------------------------------------ |
| `dlt`           | `int`    | 4 bytes  | Data Link Type, identifies the link-layer protocol of the capture source |
| `timestamp`     | `long`   | 8 bytes  | Packet capture timestamp in milliseconds since Unix Epoch                |
| `rawPacketData` | `byte[]` | variable | Raw packet bytes as captured by Pcap4j                                   |
This binary format defines the interface between any packet source and `flink-processor`. Any producer that emits records in this format is compatible with the processing pipeline.

#### Timestamp and DLT
Both values are recorded at the point of capture by `kafka-producer`. For PCAP file replay, `PcapFileReader` reads `handle.getDlt().value()` once per file and `handle.getTimestamp().getTime()` per packet. For live capture, `ProducerService` reads both values inline within the `PacketListener` callback for each arriving packet. In both cases the values are written directly into the binary record before Kafka ingestion.

The field `captureTimestamp`  of the `NetworkPacket` reflects the original capture time as recorded by the operating system or embedded in the PCAP file, not the time of Kafka ingestion or Flink processing. This distinction is relevant for any algorithm that performs time-based analysis on inter-arrival times or flow duration.


### 3.3 NetworkPacket

A `NetworkPacket` represents a single captured network packet as it flows through the `flink-processor`. It is constructed by `PcapPacketDeserializer` during deserialization of an incoming binary record and is the primary data object passed to the plugin.

| Field              | Type                 | Description                                                                                       |
| ------------------ | -------------------- | ------------------------------------------------------------------------------------------------- |
| `rawPacketData`    | `byte[]`             | Raw packet bytes as captured, corresponding to the `rawPacketData` field of the binary record.    |
| `dltId`            | `int`                | Data Link Type ID, used to correctly parse `rawPacketData` into a Pcap4j `Packet`.                |
| `captureTimestamp` | `long`               | Capture timestamp in milliseconds since Unix Epoch                                                |
| `customKey`        | `String`             | Flow grouping key assigned by the `PacketKeyExtractor` used by `keyBy()` to partition the stream. |
| `packetCache`      | `Packet` (transient) | Lazily parsed Pcap4j `Packet` object, not serialized.                                             |

#### Lazy Parsing 
`NetworkPacket` does not transmit a parsed protocol object through Kafka and Flink. Only `rawPacketData`, `dltId`, and `captureTimestamp` are serialized. Protocol parsing is deferred until the plugin explicitly calls `getRawPacket()`, at which point Pcap4j reconstructs the full packet tree using `PacketFactories` with the `DataLinkType` derived from `dltId`. The parsed object is cached in `packetCache` for the lifetime of the JVM instance, so repeated calls to `getRawPacket()` within the same `processFlow()` invocation carry no additional parsing cost. 

`getRawPacket()` returns `null` if parsing fails, for example due to a malformed packet or an unsupported protocol. Plugin implementations must therefore handle a `null` return value. 

### 3.4 DetectionResult

 `DetectionResult` is the output produced by a plugin's `detect()` method. It is written by `flink-processor` to the filesystem as JSONL through a `FileSink`.
 
  The mandatory fields `algorithmName`, `flowId`, and `timestamp` are set at construction time. `timestamp` is set automatically to `System.currentTimeMillis()` when the object is constructed. It reflects the time of result production, not the capture time of the analysed packets. All algorithm-specific output is stored in `analysisDetails`, which the plugin populates freely using the `addDetail(key, value)` method. 
A return value of `null` suppresses output for that window or packet and is intended.

| Field             | Type                  | Description                                                                           |
| ----------------- | --------------------- | ------------------------------------------------------------------------------------- |
| `algorithmName`   | `String`              | Identifier of the producing plugin.                                                   |
| `flowId`          | `String`              | The key of the analysed flow, as assigned by the `PacketKeyExtractor`.                |
| `timestamp`       | `long`                | Result timestamp in milliseconds since Unix Epoch. Set automatically at construction. |
| `analysisDetails` | `Map<String, Object>` | Plugin-defined output data, content and structure are determined by the plugin.       |

Example output as written to JSONL:
```
{"algorithmName":"cabukv1","flowId":"192.168.1.1:443->10.0.0.1:80",
"timestamp":1742838000000,"analysisDetails":{"A":1,"B":"xyz"}}
```
---

## Chapter 4 Modules

## 4.1 detection-api

The `detection-api` module defines the public contract between the framework and detection algorithm plugins. It is the only module a plugin developer needs. The framework and plugin JARs are compiled independently. The interface binds them at runtime through Flink's ClassLoader mechanism described in Section 5.2.

### Classes

**Package `com.covertchannel.framework.api`**

| Class                    | Type                 | Responsibility                                                                                                                                                                                                                                                                           |     |
| ------------------------ | -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --- |
| `DetectionAlgorithm`     | Interface            | Defines the full plugin contract including required lifecycle methods and optional state management methods, extends `Serializable` for Flink compatibility.                                                                                                                             |     |
| `PacketKeyExtractor`     | Functional Interface | Defines the single method `getKey(NetworkPacket)`, returning a `String` flow-partition key. Extends `Serializable` for stable Flink serialisation, is returned by `getKeyExtractor()`.                                                                                                   |     |
| `PacketFeatureExtractor` | Functional Interface | Defines the single method `extract(NetworkPacket)`, returning a `PacketFeatures` object. Extends `Serializable`. Returned by `getFeatureExtractor()` and used in the Feature-Based Window Processing path.                                                                               |     |
| `FrameworkConfig`        | Class                | Wraps a `Map<String, Object>` of algorithm parameters and exposes typed accessors (`getString`, `getInt`, `getDouble`, `getBoolean`) with default value support. Passed to `initialize()` at algorithm start, implements `Serializable`.                                                 |     |
| `JobContext`             | Class                | Constructed from a flat `Map<String, Object>` parsed from `config.json`. Holds explicitly typed framework topology parameters (window type, Kafka topic, parallelism) and exposes `getParam(key)` for algorithm-specific values, is consumed by `AlgorithmJobFactory` at job submission. |     |

**Package `com.covertchannel.processor`**

| Class             | Type  | Responsibility                                                                                                                                                                                                                                                |
| ----------------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `NetworkPacket`   | Class | Wraps raw packet bytes, DLT identifier, and capture timestamp. Provides lazy Pcap4j `Packet` parsing via `getRawPacket()`, the data passed to `processFlow()`.                                                                                                |
| `DetectionResult` | Class | Output record produced by `detect()`. Carries mandatory fields `algorithmName`, `flowId`, and `timestamp` set at construction. All algorithm-specific output is stored in the `analysisDetails` map through `addDetail(key, value)`                           |
| `PacketFeatures`  | Class | Lightweight data carrier produced by `PacketFeatureExtractor`. Holds the pre-extracted fields required by a specific algorithm. Used in the Feature-Based Window Processing path to avoid retaining full `NetworkPacket` objects across window accumulation\| |


### DetectionAlgorithm Interface

`DetectionAlgorithm` is the sole dependency for plugin development. Every detection algorithm must implement this interface and be packaged as a standalone JAR. The interface extends `Serializable`, which is required because Flink serializes operator state across the network and to disk during checkpointing.
The interface defines three execution paths. The framework selects the active path at job construction time based on the `windowType` configuration parameter and the return value of `supportsFeatureExtraction()`.

#### Execution Paths

| Execution Path                      | Condition                                               | Processing Sequence                                                                                                                                                                                                                       |
| ----------------------------------- | ------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Packet Processing**               | `windowType: none`                                      | Per packet, `processFlow(packet)`, then `detect()` immediately. The state must be managed by the algorithm alone.                                                                                                                         |
| **Buffered Window Processing**      | windowed, `supportsFeatureExtraction()` returns `false` | The framework buffers all `NetworkPacket` objects in the window. At window close: `processFlow(packet)` is called for each buffered packet, then `detect()` once.                                                                         |
| **Feature-Based Window Processing** | windowed, `supportsFeatureExtraction()` returns `true`  | Per packet, `getFeatureExtractor().extract(packet)` is called and the resulting `PacketFeatures` is buffered by Flink. At window close: `processFlowFeatures(features)` is called for each buffered feature object, then `detect()` once. |

Depending on the selected execution path, either `processFlow()` or `processFlowFeatures()` must be overridden to receive and process data. The interface does not enforce this at compile time.

#### Required Methods

| Method                                      | When Called                   | Responsibility                                                                                             |
| ------------------------------------------- | ----------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `void initialize(FrameworkConfig config)`   | Once at algorithm start       | Reads algorithm-specific parameters from `FrameworkConfig` and initialises internal state.                 |
| `DetectionResult detect() throws Exception` | At window close or per packet | Computes and returns a `DetectionResult` based on accumulated state, may return `null` to suppress output. |
| `void close()`                              | Once at job end               | Releases resources such as open file handles or network connections.                                       |

#### Extension Methods and Feature Extraction

These methods carry default implementations and should be overridden selectively depending on the chosen execution path.

| Method                                              | Default                                | Execution Path                          | Responsibility                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| --------------------------------------------------- | -------------------------------------- | --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `void processFlow(NetworkPacket packet)`            | no-op                                  | Packet Processing, Buffered Window      | Called per packet if`windowType: none`Override to receive individual `NetworkPacket`objects.<br>Otherwise called per accumulated `NetworkPacket`  at window close.                                                                                                                                                                                                                                                                                                         |
| `void processFlowFeatures(PacketFeatures features)` | throws `UnsupportedOperationException` | Feature-Based Window                    | Called per accumulated `PacketFeatures` at window close. Must be overridden when `supportsFeatureExtraction()` returns `true`.                                                                                                                                                                                                                                                                                                                                             |
| `void resetAfterWindow()`                           | no-op                                  | Buffered Window, Feature-Based Window\| | Called by the framework after each window result is emitted. Override to clear per-window state.                                                                                                                                                                                                                                                                                                                                                                           |
| `boolean supportsFeatureExtraction()`               | `false`                                | --                                      | Returns `true` to activate the Feature-Based Window Processing path.                                                                                                                                                                                                                                                                                                                                                                                                       |
| `PacketFeatureExtractor getFeatureExtractor()`      | timestamp extractor                    | Feature-Based Window                    | Returns the `PacketFeatureExtractor` used to reduce each `NetworkPacket` to a `PacketFeatures` object before window accumulation. The default implementation retains only the capture timestamp. Override to extract the fields required by the algorithm, so that the full raw packet payload may be garbage-collected immediately after extraction. This method must be overridden in conjunction with `supportsFeatureExtraction()` to produce meaningful feature data. |
| `PacketKeyExtractor getKeyExtractor()`              | `srcIP->dstIP` extractor               | all paths                               | Returns the `PacketKeyExtractor` used by `keyBy()` to partition the stream into flows. See _Default Key Extraction_ below.                                                                                                                                                                                                                                                                                                                                                 |

#### Default Key Extraction

`DetectionAlgorithm` provides a default implementation of `getKeyExtractor()` that returns a `PacketKeyExtractor` extracting the source-to-destination IP address string in the format 
`srcIP->dstIP`. The method attempts IPv4 first, then IPv6. If neither IP layer is present, for example in ARP or raw Ethernet frames, it returns the string `"Unknown"`. If the packet is `null` or parsing fails, it returns `"null"`. Algorithms that require a different partitioning strategy, such as keying by five-tuple or by destination port, override this method and return a custom `PacketKeyExtractor` implementation.

Packet ordering within a flow is only guaranteed within a single Kafka partition. The Kafka producer publishes records keyed by source IP address, meaning all packets originating from the same source address are routed to the same partition and arrive in order. Key extractors that are based on the source IP address, such as the default `srcIP->dstIP` key, preserve this ordering guarantee, as all packets for any given flow originate from a single partition. Key extractors that do not incorporate the source IP address, for example extractors keying solely by destination IP or by destination port, may aggregate packets from multiple Kafka partitions into a single logical flow. Such implementations cannot rely on strict arrival order and must be designed to tolerate out-of-order packets accordingly.

---
### 4.2 kafka-producer
The `kafka-producer` module publishes network packets to a Kafka topic as binary records. It supports two input modes: PCAP file replay and live network interface capture. Configuration parameters are listed in Section 6.

#### Classes

**Package `com.covertchannel.producer`**

| Class                                | Type        | Responsibility                                                                                                                                                                                                                                                                                                   |
| ------------------------------------ | ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `KafkaPacketProducer`                | Producer    | Wraps a Kafka `KafkaProducer<String, byte[]>`, serializes each captured packet into the binary record format (Section 3.2) by `sendPacketAsync()`.                                                                                                                                                               |
| `PcapFileReader`                     | Reader      | Opens a PCAP file via `PcapHandle`, reads packets sequentially, and forwards each to `KafkaPacketProducer`. Supports a configurable `maxPackets` limit and a consecutive failure threshold that aborts processing if too many packets fail. Returns a `ProcessingResult` with packet and byte counts.            |
| `PcapToKafkaApplication`             | Entry Point | CLI entry point for PCAP file replay. Accepts a file path or directory, resolves all `.pcap`/`.pcapng` files, and drives `PcapFileReader` for each. Supports an optional global `maxPackets` limit across files.                                                                                                 |
| `NetworkInterfaceToKafkaApplication` | Entry Point | CLI entry point for live capture, opens a named network interface via Pcap4j in promiscuous mode, captures packets in a loop, and forwards each to `KafkaPacketProducer`. Registers a `ShutdownHook` for graceful `SIGINT`/`SIGTERM` handling that closes the Pcap handle and flushes the Kafka producer buffer. |

**Partition Key Strategy**

Each binary record is published to Kafka with the source IP address of the captured packet as the record key. This ensures that all packets originating from the same source host are routed to the same Kafka partition and arrive in order. Packets that carry no IP layer, such as ARP or raw Ethernet frames, are published with a `null` key and are distributed across partitions by Kafka's default round-robin strategy. Ordering guarantees do not apply to these packets.

---

### 4.3 flink-processor

The `flink-processor` module consumes `NetworkPacket` records from Kafka, constructs and executes the detection pipeline, and writes `DetectionResult` records to the filesystem. The module is compiled as a library and is invoked by `framework-rest-api` at job submission time. The central entry point is `AlgorithmJobFactory`, which assembles the Flink `StreamGraph` based on the supplied `JobContext`.

#### Classes

**Package `com.covertchannel.framework`**

| Class                                         | Type                  | Responsibility                                                                                                                                                                               |
| --------------------------------------------- | --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `AlgorithmJobFactory`                         | Class                 | Assembles and returns a Flink `StreamGraph` for a given algorithm JAR and `JobContext`. Selects the execution mode and pipeline path, and wires Kafka source, windowed operators, and sinks. |
| `AlgorithmJobFactory.WindowType`              | Enum (inner)          | Enumerates the supported window configurations.                                                                                                                                              |
| `AlgorithmJobFactory.CachedClassLoader`       | Class (private inner) | Pairs a `URLClassLoader` with the last-modified timestamp of its source JAR.                                                                                                                 |
| `AlgorithmJobFactory.KeyExtractorMapFunction` | Class (private inner) | `RichMapFunction` that sets the `customKey` field on each `NetworkPacket` using the algorithm's `PacketKeyExtractor`.                                                                        |
| `CephaConfig`                                 | Class                 | Centralises environment variable names and default values for output paths and file rollover configuration.                                                                                  |
| `DetectionSinkFactory`                        | Class                 | Constructs the Flink `FileSink` for JSONL result output using output path and rollover parameters from `CephaConfig`.                                                                        |
**Package `com.covertchannel.processor`**

| Class                                   | Type           | Responsibility                                                                                                                                                                                                      |
| --------------------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `AbstractBuffDetectionProcessWindow<W>` | Abstract Class | Base class for the Buffered Window Processing path. Manages algorithm lifecycle and drives the `processFlow() → detect() → resetAfterWindow()` sequence at window close.                                            |
| `BuffTimeDetectionProcessWindow`        | Class          | Concrete subclass of `AbstractBuffDetectionProcessWindow` for time-based windows.                                                                                                                                   |
| `BuffGlobalDetectionProcessWindow`      | Class          | Concrete subclass of `AbstractBuffDetectionProcessWindow` for count-based windows.                                                                                                                                  |
| `AbstractIncrDetectionProcessWIndow<W>` | Abstract Class | Base class for the Feature-Based Window Processing path. Manages algorithm lifecycle and drives the `processFlowFeatures() → detect() → resetAfterWindow()` sequence at window close.                               |
| `IncrTimeDetectionProcessWindow`        | Class          | Concrete subclass of `AbstractIncrDetectionProcessWIndow` for time-based windows.                                                                                                                                   |
| `IncrGlobalDetectionProcessWindow`      | Class          | Concrete subclass of `AbstractIncrDetectionProcessWIndow` for count-based windows.                                                                                                                                  |
| `DetectionMapFunction`                  | Class          | `FlatMapFunction` for the Packet Processing path(`windowType: none`) Calls `processFlow()` and `detect()` per packet and emits non-null results.                                                                    |
| `KeyNFeatureExtractorMapFunction`       | Class          | `RichMapFunction` for the Feature-Based Window Processing path. Resolves `PacketKeyExtractor` and `PacketFeatureExtractor` from the algorithm JAR and maps each `NetworkPacket` to a keyed `PacketFeatures` object. |
| `PcapPacketDeserializer`                | Class          | Flink `DeserializationSchema` that deserialises binary Kafka records into `NetworkPacket` objects.                                                                                                                  |
| `PartialCountWindowBatchTrigger<W>`     | Class          | Custom window trigger for `BATCH` mode. Fires incomplete count windows at end-of-input when `partialFlush=true`.                                                                                                    |
### 4.4 framework-rest-api

The `framework-rest-api` module is a Spring Boot application that acts as the central control system of the framework. It exposes a REST interface through which plugin JARs and configuration files are managed, Flink jobs are submitted, and Kafka topics and packet producers are controlled. It is the primary operational interface and replaces direct access to the Flink cluster or the command line. Job monitoring, runtime status, cancellation, and error diagnostics of running Flink jobs, is handled exclusively through the native Flink Dashboard and is not duplicated by this module.

## Classes

|Class|Type|Responsibility|
|---|---|---|
|`RestApplication`|Entry Point|Spring Boot application entry point; bootstraps the application context|
|`FlinkConfiguration`|Configuration|Spring `@Bean` factory for `RestClusterClient` and `AlgorithmJobManager`; reads Flink host and port from `application.yml`|
|`AlgorithmSubmissionRestApi`|Controller|Plugin and job endpoints; delegates file management to `AlgorithmJarManager` and job submission to `AlgorithmJobManager`|
|`KafkaAdminRestController`|Controller|Kafka topic and consumer group endpoints; delegates to `KafkaAdminService`|
|`ProducerRestController`|Controller|Producer endpoints; delegates to `ProducerService`|
|`AlgorithmJarManager`|Service|Filesystem management for JARs and `config.json` under `/opt/flink-plugins/{algorithmId}/`; evicts cached class loaders on delete and overwrite operations|
|`AlgorithmJobManager`|Service|Submits a Flink `JobGraph` to the cluster via `RestClusterClient`; internally constructs a `JobContext` from the configuration map of the submission|
|`KafkaAdminService`|Service|Kafka Admin Client for topic management and consumer group queries; automatically creates configured default topics on startup if specified in `application.yml`|
|`ProducerService`|Service|Thread-safe management of live capture and file replay sessions; supports exactly one active session at a time; performs graceful shutdown via `@PreDestroy`|
|`KafkaProperties`|Configuration Binding|`@ConfigurationProperties` bean for the prefix `kafka`; injected into `KafkaAdminService`; binds `bootstrapServers`, `admin.autoCreateTopics`, and `admin.defaultTopics`|
|`AlgorithmJobSubmission`|DTO|Internal transfer object between `AlgorithmSubmissionRestApi` and `AlgorithmJobManager`; carries `algorithmId`, `jarPath`, and the raw configuration map|

## REST Endpoints

JAR and configuration are uploaded separately and stored under `/opt/flink-plugins/{algorithmId}/`. The `config.json` is the single source for all job parameters including Kafka brokers, topics, and windowing configuration; these are read from the file at job execution time and are not passed directly via the API call.

**Plugin and Job Management** (`/api/algorithms`)

| Method   | Endpoint                       | Description                                              |
| -------- | ------------------------------ | -------------------------------------------------------- |
| `POST`   | `/upload-jar/{algorithmId}`    | Upload a plugin JAR                                      |
| `POST`   | `/upload-config/{algorithmId}` | Upload a `config.json`                                   |
| `GET`    | `/list`                        | List all uploaded algorithms with JAR and config status  |
| `POST`   | `/{algorithmId}/execute`       | Submit and start a Flink job for the specified algorithm |
| `DELETE` | `/{algorithmId}`               | Delete the algorithm directory including JAR and config  |
| `DELETE` | `/{algorithmId}/jar`           | Delete JAR only                                          |


**Kafka Administration** (`/api/kafka`)

|Method|Endpoint|Description|
|---|---|---|
|`GET`|`/health`|Check Kafka connection status|
|`GET`|`/topics`|List all topics|
|`GET`|`/topics/{topicName}`|Get partition count, replication factor, retention, and message count for a topic|
|`POST`|`/topics`|Create a new topic with configurable partitions, replication factor, and retention|
|`DELETE`|`/topics/{topicName}`|Delete a topic|
|`GET`|`/consumer-groups`|List all consumer groups|
|`GET`|`/consumer-groups/{groupId}`|Get offset and member details for a consumer group|
|`DELETE`|`/consumer-groups/{groupId}`|Delete an inactive consumer group. The group must be in `EMPTY` state; active groups are rejected.|

**Producer Control** (`/api/kafka/producer`)

| Method | Endpoint      | Description                                                                             |     |
| ------ | ------------- | --------------------------------------------------------------------------------------- | --- |
| `GET`  | `/status`     | Return current producer state (running/stopped, mode, active interface or file, topic). |     |
| `GET`  | `/interfaces` | List all network interfaces available on the producer host.                             |     |
| `GET`  | `/files`      | List all PCAP files available in the configured PCAP directory.                         |     |
| `POST` | `/start-live` | Start live capture,`bpfFilter` is optional.                                             |     |
| `POST` | `/start-file` | Start PCAP file replay.`maxPackets: 0` means unlimited.                                 |     |
| `POST` | `/stop`       | Stop the currently running producer.                                                    |     |

## Chapter 5 The Processing Pipeline

The `flink-processor` module is the runtime execution engine of the framework. It constructs
the Flink `StreamGraph` at job submission time, loads algorithm plugins dynamically from
uploaded JARs, applies configurable windowing strategies, and routes detection results to
their output sinks. This chapter documents the internal mechanics of each stage in detail.

The design of the processing pipeline is governed by five principles:

- **Separation of Concerns**: Each class has a single responsibility.
  `AlgorithmJobFactory` builds the graph. `DetectionSinkFactory` configures output and
  window functions process data. 

- **Plugin Isolation**: Each algorithm JAR is loaded in its own `URLClassLoader`.
  A plugin failure cannot corrupt the classloading state of other jobs running on
  the same TaskManager.

- **Configurability over Code**: Window type, parallelism, execution mode (STREAMING
  vs. BATCH), and all algorithm parameters are resolved at job submission time from
  a `JobContext` object. No detection-specific values are hardcoded in the framework.

- **Extensibility by Contract**: The `DetectionAlgorithm` interface is the sole
  extension point. Adding a new detection algorithm requires no modification to the
  framework itself.

- **Pluggable Output**: `DetectionSinkFactory` decouples result serialization and
  sink configuration from detection logic, allowing output formats to be changed
  independently.

The following sections describe each stage of the pipeline in order of execution:
source construction (Section 5.1), plugin loading and ClassLoader isolation (Section 5.2),
windowing strategies (Section 5.3), the algorithm lifecycle within a window (Section 5.4),
and incremental state aggregation (Section 5.5).

### 5.1 StreamGraph Construction

The `AlgorithmJobFactory` class is the entry point for creating a runnable Flink
job. Its public method `createJobGraph(String algorithmJarPath, JobContext context)` accepts the local path to the algorithm JAR and a fully populated `JobContext` object, and returns
a `StreamGraph` that `framework-rest-api` submits to the Flink cluster via
`RestClusterClient`. The factory itself holds no reference to running jobs and carries
no per-job state.

The `JobContext` serves a dual purpose, it provides the framework with the parameters
it needs to construct the `StreamGraph` such as window type, parallelism, algorithmClassName and also acts as the configuration carrier passed to the algorithm plugin itself. 
Algorithm-specific parameters are stored in the `.json` and can be accessed by the algorithm at initialisation time through `context.getParam(String key)`. This means a single `config.json` file configures both the framework topology and the algorithm logic.

Before the `StreamGraph` is assembled, `AlgorithmJobFactory` probes the algorithm by loading it once on the client side and calling `supportsFeatureExtraction()`. The result determines which of the two pipeline paths is constructed, the Buffered or the Feature-Based as described in Section 5.3.

#### JobContext Parameters

`JobContext` is constructed from a flat `Map<String, Object>` parsed from `config.json`.
It exposes two categories of parameters:

**Core parameters**: Are consumed by the framework to build the pipeline:

| Parameter            | Type      | Default       | Description                                                                                        |
| -------------------- | --------- | ------------- | -------------------------------------------------------------------------------------------------- |
| `algorithmClassName` | `String`  | -             | Fully qualified class name of the algorithm to load                                                |
| `inputTopic`         | `String`  | -             | Kafka topic from which `NetworkPacket` records are consumed                                        |
| `outputTopic`        | `String`  | -             | Reserved for future Kafka sink output                                                              |
| `windowType`         | `String`  | `"tumbling"`  | Window strategy; see Section 5.3                                                                   |
| `windowSizeMs`       | `long`    | `5000`        | Time window size in milliseconds                                                                   |
| `slideMs`            | `long`    | `0`           | Slide interval in milliseconds for sliding time windows                                            |
| `windowCount`        | `long`    | `0`           | Window size in number of packets for count-based windows                                           |
| `slideCount`         | `long`    | `0`           | Slide size in packets for count-based sliding windows                                              |
| `executionMode`      | `String`  | `"STREAMING"` | Execution mode; see Section 5.1.1                                                                  |
| `parallelism`        | `int`     | Flink default | Job-level operator parallelism                                                                     |
| `partialFlush`       | `boolean` | `false`       | Fires incomplete count windows at end-of-input, used only in `BATCH` mode with count-based windows |

**Algorithm-specific parameters**: All remaining keys in `config.json` are forwarded
to the algorithm with. The framework does not validate or inspect these values.

##### Window Types

The `windowType` parameter accepts the following values:

|Config Value|`WindowType`|Description|
|---|---|---|
|`"none"`|`NONE`|Per-packet processing without windowing|
|`"tumbling"`|`TUMBLING_WINDOW`|Non-overlapping time windows of fixed size|
|`"sliding"`|`SLIDING_WINDOW`|Overlapping time windows with configurable slide interval|
|`"session"`|`SESSION_WINDOW`|Time windows separated by an inactivity gap|
|`"tumbling_count"`|`TUMBLING_COUNT`|Non-overlapping count-based windows|
|`"sliding_count"`|`SLIDING_COUNT`|Overlapping count-based windows with configurable slide count|

If `windowType` is unset or unrecognised, `TUMBLING_WINDOW` is used as the default. Window configuration and operator wiring are described in Section 5.5.

##### Pipeline Stages

`createJobGraph` constructs the `StreamGraph` in five sequential stages.

| Step | Stage                     | Responsibility                                                                                                                             |
| ---- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| 1    | Environment setup         | Initialises `StreamExecutionEnvironment`, sets execution mode and state backend, registers the algorithm JAR in Flink's Distributed Cache. |
| 2    | `createKafkaSource`       | Builds the `KafkaSource<NetworkPacket>` with mode-dependent offset strategy and bounded out-of-orderness watermarking.                     |
| 3    | `createDetectionPipeline` | Selects the Buffered or Feature-Based path and wires keying, windowing, and detection operators. See Section 5.3                           |
| 4    | `configureSinks`          | Attaches the JSONL result `FileSink` and, for windowed modes, the DLQ side-output sink.                                                    |
| 5    | `getStreamGraph`          | Finalises the operator graph into a serialisable `StreamGraph` and sets the job name.                                                      |



#### 5.1.1 Execution Modes

The execution mode controls the Flink runtime behaviour and the Kafka source offset strategy. It is set via the `executionMode` parameter in `config.json` and defaults to `STREAMING`.

| Mode        | Flink Runtime | Kafka Source                                                     | State Backend                                                 | Use Case                                                          |
| ----------- | ------------- | ---------------------------------------------------------------- | ------------------------------------------------------------- | ----------------------------------------------------------------- |
| `STREAMING` | `STREAMING`   | Unbounded. Reads from earliest offset and continues indefinitely | `HashMapStateBackend` with checkpointing                      | Live capture or continuous monitoring                             |
| `BATCH`     | `BATCH`       | Bounded. Reads earliest to latest offsets at job start           | RocksDB (configured via `state.backend.type` in `flink-conf`) | Offline analysis of a fixed dataset                               |
| `REPLAY`    | `STREAMING`   | Bounded. Reads earliest to latest offsets at job start           | `HashMapStateBackend` with checkpointing                      | Offline analysis of a fixed dataset with full streaming semantics |
`REPLAY` is a hybrid mode, the Kafka source is bounded identically to `BATCH`, consuming all records currently in the topic and then signalling end-of-stream. Unlike `BATCH`, the Flink runtime remains in `STREAMING` mode with an active `HashMapStateBackend` and checkpointing. The bounded source causes Flink to emit a final end-of-stream watermark, which ensures all open time windows close cleanly at job completion without discarding buffered packets.

The state backend for `BATCH` mode is not set programmatically by `AlgorithmJobFactory`. It is governed by the TaskManager configuration. In the reference deployment, `state.backend.type: rocksdb` is set in `docker-compose.taskmanager.yml`, with managed memory allocated per slot. This can be adjusted in the deployment configuration.

### 5.2 Plugin Loading and JAR Distribution

The algorithm JAR is distributed to TaskManagers via Flink's Distributed Cache. At job construction time, `AlgorithmJobFactory` registers the JAR once on the client side using `env.registerCachedFile(algorithmJarPath, ALGORITHM_JAR_CACHE_KEY)`. Flink then ships the file automatically to all TaskManagers via the BlobStore before the job starts. Each operator that requires the JAR retrieves it at runtime in `open()` through `getRuntimeContext().getDistributedCache().getFile(ALGORITHM_JAR_CACHE_KEY)`.

The actual class loading is handled by `AlgorithmJobFactory.loadAlgorithmFromJar()`. This method maintains a static `ConcurrentHashMap` of `CachedClassLoader` instances, keyed by JAR path. On each invocation it compares the JAR file's `lastModified` timestamp against the cached entry. If the JAR has changed on disk, the stale`URLClassLoader` is closed and evicted before a new one is created. This ensures that updated plugin versions take effect without a TaskManager restart.

### 5.3 Pipeline Paths

`AlgorithmJobFactory` constructs one of three pipeline paths based on the `windowType` parameter and the result of `supportsFeatureExtraction()`. The path is selected once at job submission time and is fixed for the lifetime of the job.

#### 5.3.1 Buffered Path

Used when `supportsFeatureExtraction()` returns `false` and a window type other than `NONE` is configured.

```text
filter(not null) 
-> KeyExtractorMapFunction (sets NetworkPacket.customKey) 
-> keyBy(NetworkPacket::getCustomKey) 
-> window(...)  
-> AbstractBuffDetectionProcessWindow

```
`KeyExtractorMapFunction` is a private inner class of `AlgorithmJobFactory`. It loads the algorithm's `PacketKeyExtractor` from the Distributed Cache in `open()` and sets the `customKey` field on each `NetworkPacket`. The Pcap4j packet cache is cleared after key extraction to reduce serialisation overhead during network transfer between operators. After `keyBy()`, Flink routes each packet to the operator slot responsible for its key, where the window buffers the raw `NetworkPacket` objects until the window fires.

#### 5.3.2 Feature-Based Path

Used when `supportsFeatureExtraction()` returns `true` and a window type other than `NONE` is configured.
```text

filter(not null)
-> KeyNFeatureExtractorMapFunction (sets key, maps NetworkPacket ->PacketFeatures) -> keyBy(PacketFeatures::getCustomKey)
-> window(...) 
-> AbstractIncrDetectionProcessWIndow

```
`KeyNFeatureExtractorMapFunction` performs two operations in a single map step. It extracts the flow key via the algorithm's `PacketKeyExtractor` and simultaneously maps the `NetworkPacket` to a `PacketFeatures` object via the algorithm's `PacketFeatureExtractor`. Both extractors are loaded from the Distributed Cache JAR in `open()`. The raw `NetworkPacket` is not forwarded beyond this operator. Only the lighter `PacketFeatures` object is keyed and buffered in the window, which reduces window state size for algorithms that require only derived features rather than full packet data.

#### 5.3.3 Per-Packet Path

Used when `windowType` is set to `"none"`, regardless of `supportsFeatureExtraction()`.
```text

filter(not null)
-> KeyExtractorMapFunction        (sets NetworkPacket.customKey)  
-> keyBy(NetworkPacket::getCustomKey)  
-> DetectionMapFunction        (processFlow() -> detect() per packet)

```
No window state is maintained by the framework. `DetectionMapFunction` calls `processFlow()` and `detect()` for each incoming packet immediately. The algorithm is responsible for managing any internal state it requires between packets within the same flow.

### 5.4 The Detection Algorithm Lifecycle

The algorithm lifecycle is managed by the window operator and follows the same sequence in both the Buffered and the Feature-Based path. The algorithm instance is created once per operator slot and reused across all windows processed by that slot.

##### Operator Lifecycle

| Phase      | Method                                                                                               | When                                                                  |
| ---------- | ---------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| Startup    | `initialize(FrameworkConfig)`                                                                        | Once in `open()`, after the JAR is loaded from the Distributed Cache. |
| Per window | `processFlow(NetworkPacket)` _(Buffered)_ or `processFlowFeatures(PacketFeatures)` *(Feature-Based)* | Once per element within the window.                                   |
| Per window | `detect()`                                                                                           | Once when the window closes, after all elements have been processed.  |
| Per window | `resetAfterWindow()`                                                                                 | Immediately after `detect()`, before the result is emitted.           |
| Shutdown   | `close()`                                                                                            | Once in `close()`, when the operator is torn down.                    |
The point at which `process()` gets invoked depends on the window type and trigger. Time based windows fire on watermark advancement whereas count based windows fire upon reaching the element threshold.

`resetAfterWindow()` is called unconditionally after every window, including on error paths. This guarantees that a failed window does not corrupt the algorithm state for subsequent windows on the same operator slot. If `resetAfterWindow()` itself throws, the exception is logged and the operator is considered potentially corrupt.

`detect()` may return `null` to suppress output for a window. The framework treats a `null` result as an intentional no-detection signal. No record is written to the result sink and no error is raised.

##### Per-Packet Path Lifecycle

In the per-packet path (`WindowType.NONE`), the sequence is condensed. `DetectionMapFunction` calls `processFlow()` followed immediately by `detect()` for each incoming packet. `resetAfterWindow()` is not called by the framework in this path. The algorithm is responsible for managing its own state within the same flow key.

##### DLQ Behaviour

Processing errors within a window are caught by the operator and routed to the Dead Letter Queue side output rather than failing the job. The DLQ tag differs by path: `buffered-detection-errors` for the Buffered path and `incremental-detection-errors` for the Feature-Based path. The per-packet path (`WindowType.NONE`) has no DLQ side output.


### 5.5 Window Types

The window type is selected via the `windowType` parameter in `config.json` and resolved at job submission time by `WindowType.fromConfig()`. Both the Buffered and the Feature-Based path support all window types. The concrete window operator class is selected based on the combination of pipeline path and window category.

##### Operator Class Matrix

| Window Category                                 | Buffered Path                      | Feature-Based Path                 |
| ----------------------------------------------- | ---------------------------------- | ---------------------------------- |
| Time-based (`TUMBLING`, `SLIDING`, `SESSION`)   | `BuffTimeDetectionProcessWindow`   | `IncrTimeDetectionProcessWindow`   |
| Count-based (`TUMBLING_COUNT`, `SLIDING_COUNT`) | `BuffGlobalDetectionProcessWindow` | `IncrGlobalDetectionProcessWindow` |

All four classes extend their respective abstract base class(`AbstractBuffDetectionProcessWindow` or `AbstractIncrDetectionProcessWIndow`) and implement only window-type-specific metadata. Time windows add `flink_window_start` and `flink_window_end` to the `DetectionResult`. Count-based windows add `window_type: "count_based"`.

##### Time-Based Windows

A window-assigner groups incoming elements into windows based on their event-time timestamp. Time-based assigners use Flink's event-time model with the packet's `captureTimestamp` as the event-time source and fire automatically when the watermark advances past the window boundary. A bounded out-of-orderness watermark strategy of 10 ms is applied at the Kafka source. The following time-based window types are supported.

|`windowType`|Assigner|Parameters|
|---|---|---|
|`"tumbling"`|`TumblingEventTimeWindows`|`windowSizeMs`|
|`"sliding"`|`SlidingEventTimeWindows`|`windowSizeMs`, `slideMs`|
|`"session"`|`EventTimeSessionWindows`|`windowSizeMs` (used as inactivity gap)|

##### Count-Based Windows

Count windows use an explicit trigger to define when the window fires. `CountTrigger` fires when the number of elements in a window reaches the configured threshold and retains state for overlapping windows. `PurgingTrigger` wraps `CountTrigger` and additionally purges the window state after firing. It is used exclusively for tumbling count windows where each element belongs to exactly one window. Sliding count windows do not use `PurgingTrigger`. Purging state after the first fire would cause subsequent overlapping windows to lose those elements, collapsing sliding behaviour into tumbling behaviour. A tumbling count window is produced when `slideCount` is zero or equal to `windowCount`. Setting `slideCount` greater than `windowCount` is rejected at job submission time.

| `windowType`       | Trigger                                     | Parameters                  |
| ------------------ | ------------------------------------------- | --------------------------- |
| `"tumbling_count"` | `PurgingTrigger(CountTrigger(windowCount))` | `windowCount`               |
| `"sliding_count"`  | `CountTrigger(slideCount)`                  | `windowCount`, `slideCount` |
For time-based windows, explicit trigger configuration is not required. Flink's event-time assigners handle state cleanup automatically after each window fires. The `PurgingTrigger` pattern is only necessary for count-based windows using `GlobalWindows`, where Flink performs no automatic state cleanup and the trigger must explicitly signal when state can be discarded.

##### Partial Flush

In `BATCH` execution mode, count-windows may not fire for the last batch of packets if the total packet count per flow key is not an exact multiple of `windowCount`. Setting `partialFlush: true` in `config.json` replaces the standard trigger with `PartialCountWindowBatchTrigger`, which fires all remaining open windows when the bounded input stream reaches its end. This ensures that the final incomplete window is not silently discarded. `partialFlush` has no effect in `STREAMING` or `REPLAY` mode.

## Chapter 6 Configuration Reference
 
 This chapter documents all configuration parameters of the CEPHA framework. It describes what each parameter controls and why it exists as a configurable value, without describing a specific deployment procedure. Concrete start-up instructions and environment specific setup are covered in the Deployment Guide. 
 
### 6.1 Deployment Architecture

CEPHA is composed of four independently deployable service groups. Each group maps to a separate Docker Compose file, which allows the topology to be scaled across multiple hosts or collapsed onto a single machine for local evaluation. 

| Compose File                     | Service Group      | Core Services                                                             |
| -------------------------------- | ------------------ | ------------------------------------------------------------------------- |
| `docker-compose.localnodes.yml`  | All-in-one (local) | All services on a single host, intended for development and evaluation    |
| `docker-compose.jobmanager.yml`  | Control plane      | Flink JobManager, CEPHA REST API, OpenObserve, OTel Collector, Fluent Bit |
| `docker-compose.kafka.yml`       | Message broker     | Kafka (KRaft mode)                                                        |
| `docker-compose.taskmanager.yml` | Execution workers  | Flink TaskManager. deployed once per worker host                          |
   
The local Compose file (`docker-compose.localnodes.yml`) is structurally identical to the distributed setup, it uses the same service definitions and network configuration, but runs all containers in a single Docker bridge network (`cepha-network`) on one machine. This means any configuration validated locally transfers directly to the distributed deployment without modification. The distributed topology separates the control plane from the execution workers. The JobManager and REST API require stable addressing because all TaskManagers and the REST client must reach them at a fixed host and port. Kafka is isolated on its own host because it is the only service that both the `kafka-producer` and the `flink-processor` depend on directly, and its network and disk I/O profile differs from that of the Flink components. 

#### 6.2 The `framework-rest-api`Environment Variables 
This container is configured exclusively through environment variables. All variables have defaults defined in `application.yml`. The Docker Compose files set the effective values for each deployment scenario.

##### Server

|Variable|Default|Description|
|---|---|---|
|`SERVER_PORT`|`8080`|HTTP port the REST API listens on|
|`SPRING_SERVLET_MULTIPART_MAX_FILE_SIZE`|`100MB`|Maximum size of a single uploaded file (JAR or PCAP)|
|`SPRING_SERVLET_MULTIPART_MAX_REQUEST_SIZE`|`100MB`|Maximum total multipart request size|

##### Flink Cluster

|Variable|Default|Description|
|---|---|---|
|`flink.cluster.host`|`localhost`|Hostname or IP of the Flink JobManager|
|`flink.cluster.port`|`6123`|RPC port of the Flink JobManager|
|`FLINK_CLUSTER_REST_PORT`|`8081`|REST port of the Flink Dashboard|
|`FLINK_DASHBOARD_URL`|—|Full URL to the Flink Dashboard, used for link generation only|
|`FLINK_TIMEOUT_MS`|`60000`|Timeout in milliseconds for REST client calls to the Flink cluster|

##### Kafka

|Variable|Default|Description|
|---|---|---|
|`KAFKA_BOOTSTRAPSERVERS`|`kafka:29092`|Kafka bootstrap server address|
|`KAFKA_ADMIN_AUTO_CREATE_TOPICS`|`true`|Whether the framework automatically creates configured default topics on startup|
|`kafka.producer.pcap-upload-dir`|`/tmp/pcap-uploads`|Directory from which PCAP files are served for file replay|

##### Plugin and Output Storage

| Variable                 | Default              | Description                                                   |
| ------------------------ | -------------------- | ------------------------------------------------------------- |
| `PLUGIN_STORAGE_DIR`     | `/opt/flink-plugins` | Root directory for algorithm JAR and `config.json` storage    |
| `PLUGIN_MAX_JAR_SIZE_MB` | `500`                | Maximum permitted size of an uploaded plugin JAR in megabytes |
| `CEPHA_OUTPUT_PATH`      | `/opt/flink/results` | Directory to which Flink writes JSONL detection result files  |
| `CEPHA_DLQ_PATH`         | `/opt/flink/dlq`     | Directory to which DLQ side-output records are written        |
### 6.3 Job Configuration (config.json)
Every plugin deployed to CEPHA is accompanied by a `config.json` file stored alongside the plugin JAR under `PLUGIN_STORAGE_DIR/{algorithmId}/`. This file is the single source of truth for a job execution, it configures both the Flink pipeline topology and the algorithm's own parameters. The REST API reads and forwards the file as-is. `JobContext` is constructed from the flat JSON map at submission time. Parameters that `AlgorithmJobFactory` recognises are extracted into typed fields. All remaining keys are retained in the `rawConfig` map and forwarded to the algorithm via `context.getParam(key)`. 
##### Framework Parameters
| Parameter            | Type      | Default       | Description                                                                                                                              |
| -------------------- | --------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `algorithmClassName` | `String`  | —             | Fully qualified class name of the algorithm to load.                                                                                     |
| `inputTopic`         | `String`  | —             | Kafka topic from which `NetworkPacket` records are consumed.                                                                             |
| `outputTopic`        | `String`  | —             | Reserved for future Kafka sink output.                                                                                                   |
| `windowType`         | `String`  | `"tumbling"`  | Window strategy, see Section 5.5                                                                                                         |
| `windowSizeMs`       | `long`    | `5000`        | Time window size in milliseconds                                                                                                         |
| `slideMs`            | `long`    | `0`           | Slide interval in milliseconds for sliding time windows                                                                                  |
| `windowCount`        | `long`    | `0`           | Window size in number of packets for count-based windows                                                                                 |
| `slideCount`         | `long`    | `0`           | Slide size in packets for count-based sliding windows                                                                                    |
| `executionMode`      | `String`  | `"STREAMING"` | Execution modes: `STREAMING`,`BATCH`,`REPLAY`                                                                                            |
| `partialFlush`       | `boolean` | `false`       | When `true` and `executionMode` is `BATCH`, fires incomplete count windows at stream end. Has no effect in `STREAMING` or `REPLAY` mode. |
| `parallelism`        | `int`     | Flink default | Job-level operator parallelism. If omitted, Flink uses the cluster default.                                                              |
### 6.4 Flink Properties
JobManager and TaskManager are configured via the `FLINK_PROPERTIES` environment variable. Standard Flink properties such as memory layout, slot count, and checkpointing are documented in the [Apache Flink Configuration Reference](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/config/). The following properties are either CEPHA-specific or deviate from Flink defaults for reasons relevant to this framework.

| Property                                | Component | Value                       | Description                                                                                                         |
| --------------------------------------- | --------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `metrics.reporter.prom.factory.class`   | JM,TM     | `PrometheusReporterFactory` | Enables the Prometheus metrics reporter, scraped by the OTel Collector                                              |
| `metrics.reporter.prom.port`            | JM,TM     | `9249`                      | Prometheus metrics port                                                                                             |
| `pekko.framesize`                       | JM        | `64m`                       | Raised above default because plugin JARs are shipped via the BlobServer                                             |
| `taskmanager.memory.jvm-metaspace.size` | TM        | `512m`                      | Raised because each algorithm JAR is loaded in its own `URLClassLoader`, consuming Metaspace for every loaded class |
| `state.backend.rocksdb.localdir`        | TM        | `${TASKMANAGER_SPILL_DIR}`  | RocksDB working directory path is configurable via `TASKMANAGER_SPILL_DIR`                                          |
| `io.tmp.dirs`                           | TM        | `${TASKMANAGER_SPILL_DIR}`  | Flink sort-spill and shuffle temp directory path is configurable via `TASKMANAGER_SPILL_DIR``                       |

# Chapter 7 Observability
CEPHA produces two categories of output. Detection results generated by algorithm plugins, and written by `flink-processor`and the operational metrics produced and exported by the Flink runtime. Both are collected in OpenObserve, which serves as the single access point for visualisation, search, and export. 
## 7.1 Observability Stack

| Component                 | Role                                                                                                                           |     |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------ | --- |
| Flink Prometheus Reporter | Exports Flink runtime metrics from JobManager and TaskManagers. (integrated in Flink)                                          |     |
| OpenTelemetry Collector   | Scrapes Flink Prometheus endpoints and forwards metrics to OpenObserve                                                         |     |
| Fluent Bit                | Forwards JSONL result files to OpenObserve, required only when results are written to local node filesystems (see Section 7.2) |     |
| OpenObserve               | Central storage and query engine for both metrics and detection results                                                        |     |

The metrics path and the result shipping path are independent. A failure in one does not affect the other.


## 7.2 Detection Result Pipeline
The TaskManagers writes detection results as JSONL using Flink's `FileSink`. `DetectionSinkFactory` constructs the sink at job submission time, using the parameters set in the Docker Compose file. If `CEPHA_OUTPUT_PATH` is not set, job submission fails with an explicit error. Results are written under the following path structure.

```text
CEPHA_OUTPUT_PATH}/{algorithmName}/{yyyy-MM-dd}/{HH-mm-ss}/part-{subtask}-{count}
```
The `algorithmName` subdirectory is appended by `DetectionSinkFactory`. The date and time subdirectories and the `part-*` files are created automatically by Flink's `FileSink` according to the configured rolling policy.

##### Environment Variables
All sink parameters are configured with environment variables on the TaskManager and `framework-rest-api` containers. 

| Variable                          | Required | Default | Description                                                                                                                                       |
| --------------------------------- | -------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CEPHA_OUTPUT_PATH`               | **Yes**  | -       | Base directory for all result output. Accepts any URI supported by Flink's filesystem abstraction (`file://`, NFS mount, `hdfs://`, `s3://` etc.) |
| `CEPHA_OUTPUT_ROLLOVER_SECONDS`   | No       | `60`    | Maximum age of an open result file before it is rolled over.                                                                                      |
| `CEPHA_OUTPUT_INACTIVITY_SECONDS` | No       | `60`    | Inactivity interval after which an open file is closed and rolled over.                                                                           |
| `CEPHA_OUTPUT_MAX_SIZE_MB`        | No       | `100`   | Maximum file size in MB before rollover.                                                                                                          |
| `CEPHA_DLQ_PATH`                  | -        |         | Base directory for Dead Letter Queue output. One subdirectory is created per `algorithmId`. If unset, the DLQ sink is silently skipped            |

##### Rolling Policy

The rolling policy depends on the execution mode:

| Mode        | Policy                      | Behaviour                                                                                           |
| ----------- | --------------------------- | --------------------------------------------------------------------------------------------------- |
| `STREAMING` | `OnCheckpointRollingPolicy` | Part-files are finalized at every checkpoint. Results become readable between checkpoints           |
| `REPLAY`    | `OnCheckpointRollingPolicy` | The final checkpoint at job end finalizes all open part-files                                       |
| `BATCH`     | `DefaultRollingPolicy`      | Part-files are rolled by size, time, or inactivity as configured by the environment variables above |

##### Output Path Scenarios
The value of `CEPHA_OUTPUT_PATH` determines both where results are written and what collection infrastructure is required to make them available in OpenObserve. 
##### Local filesystem 
`CEPHA_OUTPUT_PATH` points to a local container path, e.g. `/opt/flink/results`. Each
TaskManager writes to its own local volume. In a multi-node deployment, results are physically distributed across all TaskManager hosts. A result shipper must run on each node to forward results to OpenObserve. The CEPHA Docker Compose files implement this scenario using one Fluent Bit sidecar per node. Section 7.2.1 describes this configuration.
##### Distributed filesystem
`CEPHA_OUTPUT_PATH` is set to a distributed filesystem URI such as `hdfs://namenode:9000/cepha/results` or `s3://bucket/cepha/results`. Flink writes results directly to the distributed store from all TaskManagers.
### 7.2.1 Fluent Bit Configuration 
Fluent Bit tails the path configured in `CEPHA_OUTPUT_PATH`, matching all algorithm subdirectories and date/time partitions written by Flink's `FileSink`. It parses each JSONL record, enriches it with node-level metadata, and forwards it to OpenObserve via HTTP. Connection parameters, target stream, and credentials are injected via environment variables on the Fluent Bit container. The concrete configuration for all three deployment scenarios is described in the Deployment Guide.


## 7.3 Flink Runtime Metrics

Flink exports runtime metrics through its built-in Prometheus reporter. The reporter is configured on both the JobManager and TaskManager containers and exposes metrics on a dedicated HTTP port. The OpenTelemetry Collector scrapes these endpoints at a configured interval and forwards the metrics to OpenObserve, where they are stored alongside detection results.

## Prometheus Reporter Configuration

The Flink Prometheus reporter is activated through the following environment variables on the JobManager and TaskManager containers:

| Variable                              | Value                                                           | Description                                                      |
| ------------------------------------- | --------------------------------------------------------------- | ---------------------------------------------------------------- |
| `metrics.reporter.prom.factory.class` | `org.apache.flink.metrics.prometheus.PrometheusReporterFactory` | Activates the Prometheus reporter                                |
| `metrics.reporter.prom.port`          | `9249`                                                          | HTTP port on which metrics are exposed in Prometheus text format |


## Metrics Scrape Path

The OTel Collector is configured with two scrape targets, one per component:


```text

http://{jobmanager-host}:9249/metrics   //JobManager metrics 
http://{taskmanager-host}:9249/metrics  //TaskManager metrics (one target per node)
```

In the local single-node deployment (`docker-compose.localnodes.yml`) both targets resolve to `localhost`. In the distributed deployment, the JobManager target is static and the TaskManager targets must enumerate all worker hosts explicitly.

## Key Metric Groups

Flink organises metrics into scoped groups. The following groups are relevant for monitoring CEPHA jobs:

|Metric Group|Scope|Examples|
|---|---|---|
|`flink_jobmanager_job_*`|Per job|`uptime`, `numRestarts`, `lastCheckpointDuration`|
|`flink_taskmanager_job_task_*`|Per operator|`numRecordsIn`, `numRecordsOut`, `currentInputWatermark`|
|`flink_taskmanager_job_task_operator_*`|Per window operator|`numLateRecordsDropped`, window trigger counters|
|`flink_taskmanager_status_jvm_*`|Per TaskManager|`heap_used`, `heap_max`, `cpu_load`|

## OpenObserve Dashboard

Metrics forwarded by the OTel Collector are stored in OpenObserve under the stream name configured in the collector's exporter block. The concrete collector and dashboard configuration is described in the Deployment Guide.

## Appendix

### A. Troubleshooting

#### A.1 Deployment & Startup Issues

*(to be filled in next revision)*

#### A.2 Configuration Errors

*(to be filled in next revision)*

#### A.3 Plugin Errors

*(to be filled in next revision)*

### B. pom.xml Files

*(pom.xml files to be added in next revision after cleanup)*

---
