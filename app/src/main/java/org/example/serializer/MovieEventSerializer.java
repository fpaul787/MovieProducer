package org.example.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serializer;
import org.example.model.MovieEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MovieEventSerializer implements Serializer<MovieEvent> {

    private static final Logger logger = LoggerFactory.getLogger(MovieEventSerializer.class);
    private final ObjectMapper objectMapper;

    public MovieEventSerializer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public byte[] serialize(String topic, MovieEvent data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            logger.error("Error serializing MovieEvent: {}", e.getMessage(), e);
            throw new RuntimeException("Error serializing MovieEvent", e);
        }
    }
}
