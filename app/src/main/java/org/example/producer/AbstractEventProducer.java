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
     * Constructor for testing with injected KafkaProducer
     */
    public AbstractEventProducer(KafkaProducer<String, T> kafkaProducer, String topicName) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
    }

    /**
     * Constructor for production use with Properties config
     */
    public AbstractEventProducer(Properties config) {
        this.topicName = config.getProperty(getTopicConfigKey());
        if (this.topicName == null || this.topicName.isBlank()) {
            throw new IllegalArgumentException("Required config '" + getTopicConfigKey() + "' is not set");
        }
        this.kafkaProducer = new KafkaProducer<>(config);
    }

    /**
     * Returns the configuration key for the topic name
     * Implemented by concrete subclasses
     */
    protected abstract String getTopicConfigKey();

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
