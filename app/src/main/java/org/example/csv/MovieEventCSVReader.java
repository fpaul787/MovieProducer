package org.example.csv;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.example.model.MovieEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CSV reader for movie events
 */
public class MovieEventCSVReader {
    
    private static final Logger logger = LoggerFactory.getLogger(MovieEventCSVReader.class);
    
    // Expected CSV headers
    private static final String[] HEADERS = {
        "movie_title", "genre", "director", "release_year", 
        "rating", "duration_minutes", "event_type", "user_id"
    };
    
    /**
     * Reads movie events from a CSV file
     * 
     * @param filePath Path to the CSV file
     * @return List of MovieEvent objects
     * @throws IOException if file cannot be read
     */
    public List<MovieEvent> readMovieEvents(String filePath) throws IOException {
        List<MovieEvent> movieEvents = new ArrayList<>();
        
        logger.info("Starting to read movie events from CSV file: {}", filePath);
        
        try (FileReader reader = new FileReader(filePath);
             CSVParser csvParser = new CSVParser(reader, 
                 CSVFormat.DEFAULT.builder()
                            .setHeader()
                            .setSkipHeaderRecord(true)
                            .setIgnoreHeaderCase(true)
                            .setTrim(true)
                            .build())) {
            
            int recordCount = 0;
            int errorCount = 0;
            
            for (CSVRecord csvRecord : csvParser) {
                try {
                    MovieEvent movieEvent = parseCSVRecord(csvRecord);
                    movieEvents.add(movieEvent);
                    recordCount++;
                    
                    if (recordCount % 100 == 0) {
                        logger.debug("Processed {} records", recordCount);
                    }
                } catch (Exception e) {
                    errorCount++;
                    logger.warn("Error parsing CSV record {}: {} - Skipping record", 
                               csvRecord.getRecordNumber(), e.getMessage());
                }
            }
            
            logger.info("Successfully read {} movie events from CSV. {} errors encountered.", 
                       recordCount, errorCount);
        }
        
        return movieEvents;
    }
    
    /**
     * Parses a single CSV record into a MovieEvent object
     * 
     * @param record CSV record to parse
     * @return MovieEvent object
     * @throws IllegalArgumentException if required fields are missing or invalid
     */
    private MovieEvent parseCSVRecord(CSVRecord record) {
        try {
            String movieId = UUID.randomUUID().toString(); // Generate unique event ID
            String movieTitle = getStringValue(record, "movie_title", true);
            String genre = getStringValue(record, "genre", false);
            String director = getStringValue(record, "director", false);
            Integer releaseYear = getIntegerValue(record, "release_year", false);
            Double rating = getDoubleValue(record, "rating", false);
            Integer durationMinutes = getIntegerValue(record, "duration_minutes", false);
            String eventType = getStringValue(record, "event_type", true);
            String userId = getStringValue(record, "user_id", true);
            
            return new MovieEvent(movieId, movieTitle, genre, director, 
                                releaseYear, rating, durationMinutes, eventType, userId);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse CSV record: " + e.getMessage(), e);
        }
    }
    
    private String getStringValue(CSVRecord record, String columnName, boolean required) {
        if (!record.isMapped(columnName)) {
            if (required) {
                throw new IllegalArgumentException("Required column '" + columnName + "' is missing");
            }
            return null;
        }
        
        String value = record.get(columnName);
        if (value != null && !value.trim().isEmpty()) {
            return value.trim();
        } else if (required) {
            throw new IllegalArgumentException("Required column '" + columnName + "' is empty");
        }
        return null;
    }
    
    private Integer getIntegerValue(CSVRecord record, String columnName, boolean required) {
        String value = getStringValue(record, columnName, required);
        if (value == null) {
            return null;
        }
        
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException e) {
            if (required) {
                throw new IllegalArgumentException("Invalid integer value for '" + columnName + "': " + value);
            }
            logger.warn("Invalid integer value for column '{}': {}. Using null.", columnName, value);
            return null;
        }
    }
    
    private Double getDoubleValue(CSVRecord record, String columnName, boolean required) {
        String value = getStringValue(record, columnName, required);
        if (value == null) {
            return null;
        }
        
        try {
            return Double.valueOf(value);
        } catch (NumberFormatException e) {
            if (required) {
                throw new IllegalArgumentException("Invalid double value for '" + columnName + "': " + value);
            }
            logger.warn("Invalid double value for column '{}': {}. Using null.", columnName, value);
            return null;
        }
    }
}