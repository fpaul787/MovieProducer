package org.example.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for Kafka event producers
 * Contains all common producer logic
 */
public abstract class AbstractEventProducer<T extends Event> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String topicName;
    private final KafkaProducer<String, T> kafkaProducer;

    /**
     * Constructor for use with existing KafkaProducer instance
     */
    public AbstractEventProducer(KafkaProducer<String, T> kafkaProducer, String topicName) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
    }

    /**
     * Constructor for use with config properties
     * @param config Kafka producer configuration
     * @param topicConfigKey the property key for the topic name
     */
    public AbstractEventProducer(Properties config, String topicConfigKey) {
        if (topicConfigKey == null || topicConfigKey.isBlank()) {
            throw new IllegalArgumentException("topicConfigKey must not be null or blank");
        }
        this.topicName = config.getProperty(topicConfigKey);
        if (this.topicName == null || this.topicName.isBlank()) {
            throw new IllegalArgumentException("Required config '" + topicConfigKey + "' is not set");
        }
        this.kafkaProducer = new KafkaProducer<>(config);
    }

    /**
     * Sends an event to Kafka
     */
    public void sendEvent(T event) throws Exception {
        ProducerRecord<String, T> record = new ProducerRecord<>(topicName, event.getKey(), event);

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
