package org.example.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.example.model.MovieEvent;

/**
 * Kafka producer for movie events
 */
public class MovieEventProducer extends AbstractEventProducer<MovieEvent> {

    public MovieEventProducer(KafkaProducer<String, MovieEvent> kafkaProducer, String topicName) {
        super(kafkaProducer, topicName);
    }

    public MovieEventProducer(Properties config) {
        super(config, "topic.movies");
    }

    public MovieEventProducer(Properties config, String topicConfigKey) {
        super(config, topicConfigKey);
    }
}
