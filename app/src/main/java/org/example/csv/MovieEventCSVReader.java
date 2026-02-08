package org.example.csv;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
            String movieId = getStringValue(record, "movieId", true);
            String title = getStringValue(record, "title", true);
            String genres = getStringValue(record, "genres", false);
            
            return new MovieEvent(movieId, title, genres);
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
}