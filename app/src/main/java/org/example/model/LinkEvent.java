package org.example.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LinkEvent implements Event {
    @JsonProperty("movieId")
    private String movieId;

    @JsonProperty("imdbId")
    private String imdbId;

    @JsonProperty("tmdbId")
    private String tmdbId;
    
    public LinkEvent() {
    }
    
    public LinkEvent(String movieId, String imdbId, String tmdbId) {
        this.movieId = movieId;
        this.imdbId = imdbId;
        this.tmdbId = tmdbId;
    }

    public String getMovieId() { return movieId; }
    public void setMovieId(String movieId) { this.movieId = movieId; }

    public String getImdbId() { return imdbId; }
    public void setImdbId(String imdbId) { this.imdbId = imdbId; }

    public String getTmdbId() { return tmdbId; }
    public void setTmdbId(String tmdbId) { this.tmdbId = tmdbId; }

    @Override
    public String getKey() {
        return movieId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LinkEvent that = (LinkEvent) o;
        return Objects.equals(movieId, that.movieId) &&
               Objects.equals(imdbId, that.imdbId) &&
               Objects.equals(tmdbId, that.tmdbId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(movieId, imdbId, tmdbId);
    }

    @Override
    public String toString() {
        return "LinkEvent{" +
                "movieId='" + movieId + '\'' +
                ", imdbId='" + imdbId + '\'' +
                ", tmdbId='" + tmdbId + '\'' +
                '}';
    }
}
