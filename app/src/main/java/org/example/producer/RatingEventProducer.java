package org.example.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.model.RatingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RatingEventProducer {

    private static final Logger logger = LoggerFactory.getLogger(RatingEventProducer.class);
    private static final String TOPIC_NAME = "rating_events_test";
    
    private final KafkaProducer<String, RatingEvent> kafkaProducer;

    public RatingEventProducer(KafkaProducer<String, RatingEvent> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public RatingEventProducer(Properties config) {
        this.kafkaProducer = new KafkaProducer<>(config);
    }

    public void sendEvent(RatingEvent event) throws Exception {
        ProducerRecord<String, RatingEvent> record = new ProducerRecord<>(TOPIC_NAME, event.getKey(), event);
        
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
