package org.example.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.example.model.LinkEvent;


public class LinkEventProducer extends AbstractEventProducer<LinkEvent> {
    public LinkEventProducer(KafkaProducer<String, LinkEvent> kafkaProducer, String topicName) {
        super(kafkaProducer, topicName);
    }

    public LinkEventProducer(Properties config) {
        super(config, "topic.links");
    }

    public LinkEventProducer(Properties config, String topicConfigKey) {
        super(config, topicConfigKey);
    }
}
