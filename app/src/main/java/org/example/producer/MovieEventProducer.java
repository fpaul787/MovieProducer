package org.example.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.model.MovieEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MovieEventProducer implements EventProducer {

    private static final Logger logger = LoggerFactory.getLogger(MovieEventProducer.class);
    private static final String TOPIC_NAME = "movie_events_test";
    
    private final KafkaProducer<String, MovieEvent> kafkaProducer;

    public MovieEventProducer(KafkaProducer<String, MovieEvent> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public MovieEventProducer(Properties config) {
        // Initialize producer with config
        this.kafkaProducer = new KafkaProducer<>(config);
    }

    @Override
    public void sendEvent(MovieEvent event) throws Exception {
        ProducerRecord<String, MovieEvent> record = new ProducerRecord<>(TOPIC_NAME, event.getMovieId(), event);
        
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Failed to send event: {}", event.getMovieId(), exception);
            } else {
                logger.info("Sent event {} to partition {} with offset {}", 
                    event.getMovieId(), metadata.partition(), metadata.offset());
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
