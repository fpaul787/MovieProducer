package org.example.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.example.model.TagEvent;

/**
 * Kafka producer for tag events
 */
public class TagEventProducer extends AbstractEventProducer<TagEvent> {

    public TagEventProducer(KafkaProducer<String, TagEvent> kafkaProducer, String topicName) {
        super(kafkaProducer, topicName);
    }

    public TagEventProducer(Properties config) {
        super(config);
    }

    @Override
    protected String getTopicConfigKey() {
        return "topic.tags";
    }
}
