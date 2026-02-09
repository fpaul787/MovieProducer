package org.example.csv;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for CSV readers
 * Contains all common CSV reading logic
 */
public abstract class AbstractCSVReader<T> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Reads events from a CSV file
     *
     * @param filePath Path to the CSV file
     * @return List of parsed objects
     * @throws IOException if file cannot be read
     */
    public List<T> readEvents(String filePath) throws IOException {
        List<T> events = new ArrayList<>();

        logger.info("Starting to read {} from CSV file: {}", getEventTypeName(), filePath);

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
                    T event = parseCSVRecord(csvRecord);
                    events.add(event);
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

            logger.info("Successfully read {} {} from CSV. {} errors encountered.",
                       recordCount, getEventTypeName(), errorCount);
        }

        return events;
    }

    /**
     * Parses a single CSV record into an object of type T
     * Implemented by concrete subclasses
     *
     * @param record CSV record to parse
     * @return Parsed object
     * @throws IllegalArgumentException if required fields are missing or invalid
     */
    protected abstract T parseCSVRecord(CSVRecord record);

    /**
     * Returns the event type name for logging purposes
     */
    protected abstract String getEventTypeName();

    // ==================== Helper Methods ====================

    protected String getStringValue(CSVRecord record, String columnName, boolean required) {
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

    protected Double getDoubleValue(CSVRecord record, String columnName, boolean required) {
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

    protected Long getLongValue(CSVRecord record, String columnName, boolean required) {
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
