package org.example.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.model.MovieEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MovieEventProducer implements EventProducer {

    private static final Logger logger = LoggerFactory.getLogger(MovieEventProducer.class);
    private final String topicName;
    private final KafkaProducer<String, MovieEvent> kafkaProducer;

    public MovieEventProducer(KafkaProducer<String, MovieEvent> kafkaProducer, String topicName) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
    }

    public MovieEventProducer(Properties config) {
        this.kafkaProducer = new KafkaProducer<>(config);
        this.topicName = config.getProperty("topic.movies");
        if (this.topicName == null || this.topicName.isBlank()) {
            throw new IllegalArgumentException("Required config 'topic.movies' is not set");
        }
    }

    @Override
    public void sendEvent(MovieEvent event) throws Exception {
        ProducerRecord<String, MovieEvent> record = new ProducerRecord<>(topicName, event.getKey(), event);
        
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Failed to send event: {}", event.getKey(), exception);
            } else {
                logger.info("Sent event {} to partition {} with offset {}", 
                    event.getKey(), metadata.partition(), metadata.offset());
            }
        });
    }

    /**
     * Closes the Kafka producer and releases resources
     */
    public void close() {
        if (kafkaProducer != null) {
            try (kafkaProducer) {
                logger.info("Closing Kafka producer");
            }
        }
    }

}
