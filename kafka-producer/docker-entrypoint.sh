#!/bin/sh
set -e

# Default values
KAFKA_BROKERS=${KAFKA_BROKERS:-"kafka:9092"}
KAFKA_TOPIC=${KAFKA_TOPIC:-"network-flows"}
MAX_PACKETS=${MAX_PACKETS:-"0"}

echo "Starting Kafka Producer..."
echo "Mode: $CAPTURE_MODE"
echo "Brokers: $KAFKA_BROKERS"
echo "Topic: $KAFKA_TOPIC"

if [ "$CAPTURE_MODE" = "LIVE" ]; then
    INTERFACE=${INTERFACE:-"any"}
    echo "Starting Live Capture on interface: $INTERFACE"
    
    # Run the Network Interface Application
    exec java -cp /app/resources:/app/classes:/app/libs/* \
        com.covertchannel.producer.NetworkInterfaceToKafkaApplication \
        "$INTERFACE" "$KAFKA_BROKERS" "$KAFKA_TOPIC"

elif [ "$CAPTURE_MODE" = "FILE" ]; then
    PCAP_PATH=${PCAP_PATH:-"/pcap-files"}
    echo "Starting File Processing on path: $PCAP_PATH"
    
    # Run the File Reader Application
    exec java -cp /app/resources:/app/classes:/app/libs/* \
        com.covertchannel.producer.PcapToKafkaApplication \
        "$PCAP_PATH" "$KAFKA_BROKERS" "$KAFKA_TOPIC" "$MAX_PACKETS"

else
    echo "Error: CAPTURE_MODE not set to LIVE or FILE"
    exit 1
fi
