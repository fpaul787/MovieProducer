package org.example.model;

/**
 * Common interface for all events sent to Kafka
 */
public interface Event {
    
    /**
     * Returns the key to use for Kafka partitioning
     * @return the partition key
     */
    String getKey();
}
