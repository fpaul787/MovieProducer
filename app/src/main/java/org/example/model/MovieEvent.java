package org.example.model;

import java.time.LocalDateTime;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a movie event that will be sent to Kafka
 */
public class MovieEvent {
    
    @JsonProperty("movieId")
    private String movieId;
    
    @JsonProperty("title")
    private String title;
    
    @JsonProperty("genres")
    private String genres;
    
    @JsonProperty("timestamp")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime timestamp;
    
    // Default constructor
    public MovieEvent() {
        this.timestamp = LocalDateTime.now();
    }
    
    // Constructor for CSV parsing
    public MovieEvent(String movieId, String title, String genres) {
        this.movieId = movieId;
        this.title = title;
        this.genres = genres;
        this.timestamp = LocalDateTime.now();
    }
    
    // Getters and setters
    public String getMovieId() { return movieId; }
    public void setMovieId(String movieId) { this.movieId = movieId; }
    
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    
    public String getGenres() { return genres; }
    public void setGenres(String genres) { this.genres = genres; }
    
    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MovieEvent that = (MovieEvent) o;
        return Objects.equals(movieId, that.movieId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(movieId);
    }
    
    @Override
    public String toString() {
        return "MovieEvent{" +
                "movieId='" + movieId + '\'' +
                ", title='" + title + '\'' +
                ", genres='" + genres + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}