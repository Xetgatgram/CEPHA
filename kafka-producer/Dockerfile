FROM eclipse-temurin:17-jre

# Install libpcap (required for Pcap4j)
RUN apt-get update && apt-get install -y libpcap-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the built JARs (assuming maven build structure)
COPY target/kafka-producer-*.jar /app/libs/
# Copy dependencies if they are in a separate folder, or use a fat-jar
#COPY target/lib/ /app/libs/

# Copy the entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Create directory for mounting pcap files
RUN mkdir /pcap-files

ENTRYPOINT ["docker-entrypoint.sh"]