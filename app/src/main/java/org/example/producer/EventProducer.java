package org.example.producer;

import org.example.model.MovieEvent;

/**
 * Interface for producing movie events to Kafka
 */
public interface EventProducer {

    /**
     * Sends a movie event to Kafka
     * @param event
     * @throws Exception
     */
    void sendEvent(MovieEvent event) throws Exception;
}
