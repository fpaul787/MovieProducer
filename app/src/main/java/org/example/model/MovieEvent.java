package org.example.model;

import java.time.LocalDateTime;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a movie event that will be sent to Kafka
 */
public class MovieEvent {
    
    @JsonProperty("movie_id")
    private String movieId;
    
    @JsonProperty("movie_title")
    private String movieTitle;
    
    @JsonProperty("genre")
    private String genre;
    
    @JsonProperty("director")
    private String director;
    
    @JsonProperty("release_year")
    private Integer releaseYear;
    
    @JsonProperty("rating")
    private Double rating;
    
    @JsonProperty("duration_minutes")
    private Integer durationMinutes;
    
    @JsonProperty("event_type")
    private String eventType; // e.g., "view", "purchase", "rate", "favorite"
    
    @JsonProperty("user_id")
    private String userId;
    
    @JsonProperty("timestamp")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime timestamp;
    
    // Default constructor
    public MovieEvent() {
        this.timestamp = LocalDateTime.now();
    }
    
    // Constructor for CSV parsing
    public MovieEvent(String movieId, String movieTitle, String genre, String director, 
                     Integer releaseYear, Double rating, Integer durationMinutes, 
                     String eventType, String userId) {
        this.movieId = movieId;
        this.movieTitle = movieTitle;
        this.genre = genre;
        this.director = director;
        this.releaseYear = releaseYear;
        this.rating = rating;
        this.durationMinutes = durationMinutes;
        this.eventType = eventType;
        this.userId = userId;
        this.timestamp = LocalDateTime.now();
    }
    
    // Getters and setters
    public String getMovieId() { return movieId; }
    public void setMovieId(String movieId) { this.movieId = movieId; }
    
    public String getMovieTitle() { return movieTitle; }
    public void setMovieTitle(String movieTitle) { this.movieTitle = movieTitle; }
    
    public String getGenre() { return genre; }
    public void setGenre(String genre) { this.genre = genre; }
    
    public String getDirector() { return director; }
    public void setDirector(String director) { this.director = director; }
    
    public Integer getReleaseYear() { return releaseYear; }
    public void setReleaseYear(Integer releaseYear) { this.releaseYear = releaseYear; }
    
    public Double getRating() { return rating; }
    public void setRating(Double rating) { this.rating = rating; }
    
    public Integer getDurationMinutes() { return durationMinutes; }
    public void setDurationMinutes(Integer durationMinutes) { this.durationMinutes = durationMinutes; }
    
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    
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
                ", movieTitle='" + movieTitle + '\'' +
                ", genre='" + genre + '\'' +
                ", director='" + director + '\'' +
                ", releaseYear=" + releaseYear +
                ", rating=" + rating +
                ", durationMinutes=" + durationMinutes +
                ", eventType='" + eventType + '\'' +
                ", userId='" + userId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}