package org.example.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.example.model.RatingEvent;

/**
 * Kafka producer for rating events
 */
public class RatingEventProducer extends AbstractEventProducer<RatingEvent> {

    public RatingEventProducer(KafkaProducer<String, RatingEvent> kafkaProducer, String topicName) {
        super(kafkaProducer, topicName);
    }

    public RatingEventProducer(Properties config) {
        super(config);
    }

    @Override
    protected String getTopicConfigKey() {
        return "topic.ratings";
    }
}
