# Use the OpenJDK 17 Oracle image as the base
FROM openjdk:17-oracle

# Expose the port the application will run on
EXPOSE 8080

# Define the JAR file path as a build argument (adjust the default value as needed)
ARG JAR_FILE=target/finstock-0.0.1-SNAPSHOT.jar

# Copy the JAR file into the container
COPY ${JAR_FILE} /app.jar

# Define the command to run the JAR file
ENTRYPOINT ["java", "-jar", "/app.jar"]
