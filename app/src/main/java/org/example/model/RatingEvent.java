package org.example.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a rating event that will be sent to Kafka
 */
public class RatingEvent implements Event {
    
    @JsonProperty("userId")
    private String userId;
    
    @JsonProperty("movieId")
    private String movieId;
    
    @JsonProperty("rating")
    private Double rating;
    
    @JsonProperty("timestamp")
    private Long timestamp;
    
    // Default constructor
    public RatingEvent() {
    }
    
    // Constructor for CSV parsing
    public RatingEvent(String userId, String movieId, Double rating, Long timestamp) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
        this.timestamp = timestamp;
    }
    
    // Getters and setters
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    
    public String getMovieId() { return movieId; }
    public void setMovieId(String movieId) { this.movieId = movieId; }
    
    public Double getRating() { return rating; }
    public void setRating(Double rating) { this.rating = rating; }
    
    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
    
    @Override
    public String getKey() { return movieId; }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RatingEvent that = (RatingEvent) o;
        return Objects.equals(userId, that.userId) && 
               Objects.equals(movieId, that.movieId) &&
               Objects.equals(timestamp, that.timestamp);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(userId, movieId, timestamp);
    }
    
    @Override
    public String toString() {
        return "RatingEvent{" +
                "userId='" + userId + '\'' +
                ", movieId='" + movieId + '\'' +
                ", rating=" + rating +
                ", timestamp=" + timestamp +
                '}';
    }
}
