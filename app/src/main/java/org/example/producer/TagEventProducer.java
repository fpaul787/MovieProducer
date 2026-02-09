package org.example.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.model.TagEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TagEventProducer {

    private static final Logger logger = LoggerFactory.getLogger(TagEventProducer.class);
    private final String topicName;
    private final KafkaProducer<String, TagEvent> kafkaProducer;

    public TagEventProducer(KafkaProducer<String, TagEvent> kafkaProducer, String topicName) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
    }

    public TagEventProducer(Properties config) {
        this.topicName = config.getProperty("topic.tags");
        if (this.topicName == null || this.topicName.isBlank()) {
            throw new IllegalArgumentException("Required config 'topic.tags' is not set");
        }
        this.kafkaProducer = new KafkaProducer<>(config);
    }

    public void sendEvent(TagEvent event) throws Exception {
        ProducerRecord<String, TagEvent> record = new ProducerRecord<>(topicName, event.getKey(), event);

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
