package org.example.csv;

import org.apache.commons.csv.CSVRecord;
import org.example.model.RatingEvent;

/**
 * CSV reader for rating events
 */
public class RatingReader extends AbstractCSVReader<RatingEvent> {

    @Override
    protected RatingEvent parseCSVRecord(CSVRecord record) {
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

    @Override
    protected String getEventTypeName() {
        return "rating events";
    }
}
