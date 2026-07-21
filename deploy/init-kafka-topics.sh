#!/bin/bash
# init-kafka-topics.sh - Topics automatisch erstellen

set -e

echo "Warte bis Kafka bereit ist..."
sleep 3

echo "Erstelle Kafka Topics..."

# Topic 1: Network Flows (Input)
kafka-topics --bootstrap-server kafka:29092 \
  --create \
  --topic network-flows \
  --partitions 16 \
  --replication-factor 1 \
  --if-not-exists

## Topic 2: Detection Results (Output)
#kafka-topics --bootstrap-server kafka:29092 \
#  --create \
#  --topic detection-results \
#  --partitions 24 \
#  --replication-factor 1 \
#  --if-not-exists
#
## Topic 3: Alerts (Optional für kritische Detektionen)
#kafka-topics --bootstrap-server kafka:29092 \
#  --create \
#  --topic detection-alerts \
#  --partitions 1 \
#  --replication-factor 1 \
#  --if-not-exists

echo "Topics erstellt"

# Zeige Topics
kafka-topics --bootstrap-server kafka:29092 --list
