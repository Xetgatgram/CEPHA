# Plugin Development Guide

## Quick Start: Writing Your First Detection Algorithm

### Minimal Implementation

```java
public class MyDetector implements DetectionAlgorithm {
    private List<Long> timestamps = new ArrayList<>();
    
    @Override
    public void initialize(FrameworkConfig config) {
        // Optional: read custom config
    }
    
    @Override
    public void processFlow(Packet packet, long captureTimestamp) {
        // Accumulate your data
        timestamps.add(captureTimestamp);
    }
    
    @Override
    public DetectionResult detect() {
        // Analyze accumulated data
        boolean isAnomaly = timestamps.size() > 1000;
        return new DetectionResult("flow-id", isAnomaly, 1.0, "High packet rate");
    }
    
    @Override
    public void cleanup() {
        timestamps.clear();
    }
    
    @Override
    public void close() {
        cleanup();
    }
}
```

**That's it!** The framework handles:
- Kafka integration
- Windowing strategies
- Flink job orchestration
- State serialization
- Metrics collection
- Output formatting

---

## Framework Modes

### 1. Per-Packet Mode (`windowType: "none"`)

Your algorithm processes **each packet immediately** as it arrives.

```java
processFlow(packet1, ts1) → detect() → result1
processFlow(packet2, ts2) → detect() → result2
```

**Use when:**
- Real-time detection required
- Each packet is independent
- Example: Signature matching, protocol anomalies

### 2. Windowed Mode (`windowType: "tumbling/sliding/session"`)

Your algorithm **accumulates packets** in time windows, then analyzes the batch.

```java
Window 1: processFlow(pkt1) → processFlow(pkt2) → ... → detect() → result
Window 2: processFlow(pkt100) → processFlow(pkt101) → ... → detect() → result
```

**Use when:**
- Statistical analysis needed (IAT distributions, entropy)
- Flow-based detection
- Example: Compressibility, timing channels, volume anomalies

**Window Types:**
- `tumbling`: Fixed non-overlapping windows (e.g., 5s chunks)
- `sliding`: Overlapping windows (e.g., 10s window, 2s slide)
- `session`: Dynamic windows based on inactivity gaps

---

## Performance Optimization (Optional)

### Problem: Large Windows = High Memory

In windowed mode, the framework needs to preserve packets for your algorithm. For high-traffic scenarios (100k+ packets/window), this can use significant memory.

### Solution: Incremental Aggregation

Tell the framework: *"I can save my own state efficiently"*

**Add these 4 methods:**

```java
@Override
public boolean supportsIncrementalAggregation() {
    return true; // Enable optimization
}

@Override
public Serializable snapshotState() {
    // Return a compact representation of your internal state
    return new MyState(timestamps, packetCount);
}

@Override
public void restoreState(Serializable state) {
    // Restore from snapshot
    MyState s = (MyState) state;
    this.timestamps = s.timestamps;
    this.packetCount = s.packetCount;
}

@Override
public void mergeState(Serializable otherState) {
    // Combine state from parallel sub-windows
    MyState other = (MyState) otherState;
    this.timestamps.addAll(other.timestamps);
    this.packetCount += other.packetCount;
}
```

**Benefits:**
- 10-100x memory reduction (framework stops buffering raw packets)
- Enables parallel window processing
- Supports large-scale deployments

**When to use:**
- Your algorithm maintains large collections (timestamps, payloads)
- Memory profiling shows high usage
- Production deployment with high throughput

**When to skip:**
- Prototyping phase (optimize later!)
- Simple algorithms with minimal state
- Low-traffic scenarios

---

## Configuration

### Algorithm-Specific Config

Pass custom parameters via JSON:

```json
{
  "algorithmClassName": "com.example.MyDetector",
  "algorithmJarPath": "/path/to/my-detector.jar",
  "windowType": "tumbling",
  "windowSizeMs": 5000,
  "config": {
    "my_threshold": 0.7,
    "my_feature": "advanced",
    "custom_param": 42
  }
}
```

Access in your algorithm:

```java
@Override
public void initialize(FrameworkConfig config) {
    double threshold = config.getDouble("my_threshold", 0.5);
    String feature = config.getString("my_feature", "basic");
}
```

---

## Metrics and Monitoring

### Define Custom Metrics

```java
@Override
public List<MetricDefinition> getMetricDefinitions() {
    return List.of(
        new MetricDefinition("anomaly_score", MetricType.GAUGE),
        new MetricDefinition("packets_analyzed", MetricType.COUNTER),
        new MetricDefinition("detection_rate", MetricType.METER),
        new MetricDefinition("iat_distribution", MetricType.HISTOGRAM)
    );
}
```

### Populate Metrics

```java
@Override
public DetectionResult detect() {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put("anomaly_score", 0.87);
    metrics.put("packets_analyzed", 1500);
    
    return new DetectionResult("flow-id", true, 0.87, "Anomaly detected", metrics);
}
```

**Automatic export to:**
- Prometheus (via Flink metrics)
- Grafana dashboards
- JSON output files

---

## Advanced Features

### Custom Packet Keying

By default, packets are grouped by `src_ip -> dst_ip`. Override for custom logic:

```java
@Override
public PacketKeyExtractor getKeyExtractor() {
    return (packet) -> {
        IpV4Packet ip = packet.get(IpV4Packet.class);
        TcpPacket tcp = packet.get(TcpPacket.class);
        
        if (ip != null && tcp != null) {
            // Key by 5-tuple
            return ip.getHeader().getSrcAddr() + ":" + tcp.getHeader().getSrcPort() +
                   "->" +
                   ip.getHeader().getDstAddr() + ":" + tcp.getHeader().getDstPort();
        }
        return "unknown";
    };
}
```

### Access Raw Packet Data

```java
@Override
public void processFlow(Packet packet, long captureTimestamp) {
    // Full Pcap4j API available
    IpV4Packet ip = packet.get(IpV4Packet.class);
    TcpPacket tcp = packet.get(TcpPacket.class);
    
    if (tcp != null) {
        byte[] payload = tcp.getPayload().getRawData();
        int payloadSize = payload.length;
        // Analyze payload entropy, patterns, etc.
    }
}
```

---

## Migration Guide: Optimizing Existing Algorithms

### Before (Unoptimized)

```java
public class MyDetector implements DetectionAlgorithm {
    private List<Long> timestamps = new ArrayList<>(); // Grows unbounded
    
    @Override
    public void processFlow(Packet p, long ts) {
        timestamps.add(ts);
    }
}
```

**Problem:** In windowed mode, framework buffers both:
1. Raw packets (for your processFlow calls)
2. Your timestamps list

**Memory usage:** 2x packet data!

### After (Optimized)

```java
public class MyDetector implements DetectionAlgorithm {
    private MyState state = new MyState();
    
    // Enable optimization
    @Override
    public boolean supportsIncrementalAggregation() {
        return true;
    }
    
    @Override
    public Serializable snapshotState() {
        return state.copy(); // Compact serialization
    }
    
    @Override
    public void restoreState(Serializable s) {
        this.state = ((MyState) s).copy();
    }
    
    // Inner class for state
    static class MyState implements Serializable {
        List<Long> timestamps = new ArrayList<>();
        MyState copy() { /* deep copy logic */ }
    }
}
```

**Result:** Framework only stores your compact state snapshots, not raw packets.

**Memory usage:** 1x state data (10-100x smaller!)

---

## Testing Your Algorithm

### Unit Test Template

```java
@Test
public void testDetection() throws Exception {
    MyDetector detector = new MyDetector();
    
    // Initialize
    FrameworkConfig config = new FrameworkConfig(Map.of(
        "my_threshold", 0.7
    ));
    detector.initialize(config);
    
    // Simulate packet stream
    for (int i = 0; i < 100; i++) {
        Packet mockPacket = createMockPacket();
        detector.processFlow(mockPacket, System.currentTimeMillis());
    }
    
    // Verify detection
    DetectionResult result = detector.detect();
    assertNotNull(result);
    assertTrue(result.isAnomaly());
}
```

### Integration Test (with Framework)

```bash
# 1. Build your algorithm JAR
mvn clean package

# 2. Submit to framework
curl -X POST http://localhost:8080/api/algorithms/submit \
  -H "Content-Type: application/json" \
  -d '{
    "algorithmClassName": "com.example.MyDetector",
    "algorithmJarPath": "/path/to/target/my-detector.jar",
    "windowType": "tumbling",
    "windowSizeMs": 5000
  }'

# 3. Monitor output
tail -f output/MyDetector/*.jsonl
```

---

## Best Practices

### 1. **Start Simple, Optimize Later**

```java
// Phase 1: Prototype (buffered mode)
class MyDetector implements DetectionAlgorithm {
    void processFlow(...) { /* accumulate */ }
    DetectionResult detect() { /* analyze */ }
}

// Phase 2: Production (incremental mode) - add later if needed
@Override
boolean supportsIncrementalAggregation() { return true; }
```

### 2. **Handle Missing Data Gracefully**

```java
@Override
public void processFlow(Packet packet, long ts) {
    IpV4Packet ip = packet.get(IpV4Packet.class);
    if (ip == null) {
        LOG.debug("Non-IPv4 packet, skipping");
        return; // Don't crash on IPv6, ARP, etc.
    }
}
```

### 3. **Cleanup Between Windows**

```java
@Override
public void cleanup() {
    timestamps.clear(); // Reset state for next window
    packetCount = 0;
}
```

The framework calls `cleanup()` automatically between windows.

### 4. **Return Null for "No Detection"**

```java
@Override
public DetectionResult detect() {
    if (timestamps.size() < MIN_PACKETS) {
        return null; // Framework filters this out
    }
    // ... normal detection
}
```

### 5. **Use Proper Logging**

```java
private static final Logger LOG = LoggerFactory.getLogger(MyDetector.class);

@Override
public void processFlow(Packet p, long ts) {
    LOG.debug("Processing packet at {}", ts); // Debug only
    LOG.info("Anomaly detected!"); // Important events
    LOG.error("Failed to parse: {}", e.getMessage()); // Errors
}
```

---

## Example Algorithms

### 1. Simple Threshold Detector

```java
public class ThresholdDetector implements DetectionAlgorithm {
    private int packetCount = 0;
    private int threshold = 1000;
    
    @Override
    public void initialize(FrameworkConfig config) {
        threshold = config.getInt("threshold", 1000);
    }
    
    @Override
    public void processFlow(Packet packet, long ts) {
        packetCount++;
    }
    
    @Override
    public DetectionResult detect() {
        boolean anomaly = packetCount > threshold;
        return new DetectionResult("flow", anomaly, packetCount, 
            "Packet count: " + packetCount);
    }
    
    @Override
    public void cleanup() { packetCount = 0; }
    
    @Override
    public void close() { cleanup(); }
}
```

### 2. Payload Entropy Detector

```java
public class EntropyDetector implements DetectionAlgorithm {
    private List<byte[]> payloads = new ArrayList<>();
    
    @Override
    public void processFlow(Packet packet, long ts) {
        TcpPacket tcp = packet.get(TcpPacket.class);
        if (tcp != null && tcp.getPayload() != null) {
            payloads.add(tcp.getPayload().getRawData());
        }
    }
    
    @Override
    public DetectionResult detect() {
        double avgEntropy = payloads.stream()
            .mapToDouble(this::calculateEntropy)
            .average()
            .orElse(0.0);
        
        boolean anomaly = avgEntropy < 3.0; // Low entropy = suspicious
        return new DetectionResult("flow", anomaly, avgEntropy, 
            "Avg entropy: " + avgEntropy);
    }
    
    private double calculateEntropy(byte[] data) {
        int[] freq = new int[256];
        for (byte b : data) freq[b & 0xFF]++;
        
        double entropy = 0;
        for (int f : freq) {
            if (f > 0) {
                double p = (double) f / data.length;
                entropy -= p * (Math.log(p) / Math.log(2));
            }
        }
        return entropy;
    }
    
    @Override
    public void cleanup() { payloads.clear(); }
    
    @Override
    public void close() { cleanup(); }
}
```

---

## Troubleshooting

### "ClassNotFoundException" when submitting job

**Cause:** JAR path incorrect or JAR not accessible to Flink cluster.

**Fix:**
```bash
# Verify JAR exists
ls -lh /path/to/algorithm.jar

# Use absolute paths
"algorithmJarPath": "/home/user/algorithms/detector.jar"
```

### High memory usage

**Cause:** Large windows + buffered mode.

**Fix:** Implement incremental aggregation (see optimization section).

### No results in output

**Cause:** `detect()` returns null or algorithm throws exceptions.

**Fix:**
```java
@Override
public DetectionResult detect() {
    try {
        // your logic
        return new DetectionResult(...);
    } catch (Exception e) {
        LOG.error("Detection failed", e);
        return new DetectionResult("flow", false, 0.0, "Error: " + e.getMessage());
    }
}
```

### Algorithm not processing packets

**Cause:** Key extraction returns null/invalid keys.

**Fix:**
```java
@Override
public PacketKeyExtractor getKeyExtractor() {
    return (packet) -> {
        IpV4Packet ip = packet.get(IpV4Packet.class);
        if (ip == null) return "non-ipv4"; // Always return something
        return ip.getHeader().getSrcAddr().getHostAddress();
    };
}
```

---

## FAQ

**Q: Do I need to know Flink?**  
A: No! Just implement the DetectionAlgorithm interface. The framework handles Flink internals.

**Q: Can I use external libraries?**  
A: Yes! Include dependencies in your algorithm JAR (fat JAR). Use Maven shade plugin.

**Q: How do I debug my algorithm?**  
A: Add logging with SLF4J. Logs appear in Flink TaskManager logs.

**Q: Can I access raw PCAP timestamps?**  
A: Yes! The `captureTimestamp` parameter in `processFlow()` is from the PCAP file.

**Q: What if my algorithm needs multiple passes over data?**  
A: Use windowed mode and accumulate all data in `processFlow()`, then analyze in `detect()`.

**Q: Is incremental aggregation mandatory?**  
A: No! It's an optional optimization. Start with simple buffered mode.

**Q: Can I use machine learning models?**  
A: Yes! Load models in `initialize()`, run inference in `detect()`. Serialize model with state if using incremental mode.

---

## Support

- **Documentation:** See framework README.md
- **Examples:** cabuk-detector/ directory
- **Issues:** GitHub issue tracker
- **API Reference:** Javadoc in detection-api/

Happy detecting! 🚀
