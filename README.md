# Movie Producer Application

A Java application that reads movie events from CSV files, serializes them to JSON, and sends them to Apache Kafka topics.

## Features

## Prerequisites

- Java 21 or higher
- Apache Kafka cluster (for runtime)
- Gradle 9.2.1+ (for building)

## Building the Application


## CSV Format


### Field Descriptions:

## Event Schema


## Sample Data


## Dependencies

- **Apache Kafka Client**: 3.8.0
- **Apache Commons CSV**: 1.12.0
- **Jackson**: 2.18.1 (JSON processing)
- **SLF4J + Logback**: Logging
- **JUnit Jupiter**: Testing framework

## Development

### Project Structure
```
app/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── org/example/
│   │   │       ├── App.java              # Main application
│   │   │       ├── config/               # Configuration classes
│   │   │       ├── csv/                  # CSV processing
│   │   │       ├── kafka/                # Kafka integration
│   │   │       └── model/                # Data models
│   │   └── resources/
│   │       └── logback.xml              # Logging configuration
│   └── test/
└── build.gradle                        # Build configuration
```


### Debug Mode
Enable debug logging by modifying `logback.xml` or using JVM arguments:
```bash
java -jar app.jar -Dlogging.level.org.example=DEBUG
```