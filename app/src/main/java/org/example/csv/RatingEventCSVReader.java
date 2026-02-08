package org.example.csv;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.example.model.RatingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CSV reader for rating events
 */
public class RatingEventCSVReader {
    
    private static final Logger logger = LoggerFactory.getLogger(RatingEventCSVReader.class);
    
    /**
     * Reads rating events from a CSV file
     * 
     * @param filePath Path to the CSV file
     * @return List of RatingEvent objects
     * @throws IOException if file cannot be read
     */
    public List<RatingEvent> readRatingEvents(String filePath) throws IOException {
        List<RatingEvent> ratingEvents = new ArrayList<>();
        
        logger.info("Starting to read rating events from CSV file: {}", filePath);
        
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
                    RatingEvent ratingEvent = parseCSVRecord(csvRecord);
                    ratingEvents.add(ratingEvent);
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
            
            logger.info("Successfully read {} rating events from CSV. {} errors encountered.", 
                       recordCount, errorCount);
        }
        
        return ratingEvents;
    }
    
    /**
     * Parses a single CSV record into a RatingEvent object
     * 
     * @param record CSV record to parse
     * @return RatingEvent object
     * @throws IllegalArgumentException if required fields are missing or invalid
     */
    private RatingEvent parseCSVRecord(CSVRecord record) {
        try {
            String userId = getStringValue(record, "userId", true);
            String movieId = getStringValue(record, "movieId", true);
            Double rating = getDoubleValue(record, "rating", true);
            Long timestamp = getLongValue(record, "timestamp", true);
            
            return new RatingEvent(userId, movieId, rating, timestamp);
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
    
    private Double getDoubleValue(CSVRecord record, String columnName, boolean required) {
        String stringValue = getStringValue(record, columnName, required);
        if (stringValue == null) {
            return null;
        }
        try {
            return Double.parseDouble(stringValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Column '" + columnName + "' is not a valid number: " + stringValue);
        }
    }
    
    private Long getLongValue(CSVRecord record, String columnName, boolean required) {
        String stringValue = getStringValue(record, columnName, required);
        if (stringValue == null) {
            return null;
        }
        try {
            return Long.parseLong(stringValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Column '" + columnName + "' is not a valid long: " + stringValue);
        }
    }
}
