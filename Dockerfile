FROM eclipse-temurin:17-jre

# Install libpcap for packet capture support
RUN apt-get update && \
    apt-get install -y libpcap-dev && \
    rm -rf /var/lib/apt/lists/*

# Spring Boot Fat-JAR
COPY framework-rest-api/target/framework-rest-api-1.0.0.jar /opt/framework/framework-rest-api-1.0.0.jar

EXPOSE 8080

# Starte die REST API
ENTRYPOINT ["java", "-jar", "/opt/framework/framework-rest-api-1.0.0.jar"]
