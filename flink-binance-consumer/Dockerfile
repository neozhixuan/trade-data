# # ----------- Stage 1: Build the JAR -------------
# FROM maven:3.9.6-eclipse-temurin-11 AS builder

# # Create the JAR file in the /app di
# WORKDIR /app
# COPY . .
# RUN mvn clean compile package

# # ----------- Stage 2: Run -------------
# FROM openjdk:11-jre-slim

# # Copy this JAR file into our Docker image
# COPY --from=builder /app/target/flink-binance-consumer-1.0-SNAPSHOT.jar /app/app.jar

# # Run the JAR file when the container starts
# ENTRYPOINT ["java", "-jar", "/app/app.jar"]

FROM openjdk:11-jre-slim

ARG JAR_FILE=target/flink-binance-consumer-1.0-SNAPSHOT.jar

# Copy this JAR file into our Docker image
COPY ${JAR_FILE} /app/app.jar

# Run the JAR file when the container starts
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

