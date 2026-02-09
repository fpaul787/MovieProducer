package org.example.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a tag event that will be sent to Kafka
 */
public class TagEvent implements Event {
    
    @JsonProperty("userId")
    private String userId;
    
    @JsonProperty("movieId")
    private String movieId;
    
    @JsonProperty("tag")
    private String tag;
    
    @JsonProperty("timestamp")
    private Long timestamp;
    
    // Default constructor
    public TagEvent() {
    }
    
    // Constructor for CSV parsing
    public TagEvent(String userId, String movieId, String tag, Long timestamp) {
        this.userId = userId;
        this.movieId = movieId;
        this.tag = tag;
        this.timestamp = timestamp;
    }
    
    // Getters and setters
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    
    public String getMovieId() { return movieId; }
    public void setMovieId(String movieId) { this.movieId = movieId; }
    
    public String getTag() { return tag; }
    public void setTag(String tag) { this.tag = tag; }
    
    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
    
    @Override
    public String getKey() { return movieId; }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TagEvent that = (TagEvent) o;
        return Objects.equals(userId, that.userId) && 
               Objects.equals(movieId, that.movieId) &&
               Objects.equals(tag, that.tag) &&
               Objects.equals(timestamp, that.timestamp);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(userId, movieId, tag, timestamp);
    }
    
    @Override
    public String toString() {
        return "TagEvent{" +
                "userId='" + userId + '\'' +
                ", movieId='" + movieId + '\'' +
                ", tag='" + tag + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
