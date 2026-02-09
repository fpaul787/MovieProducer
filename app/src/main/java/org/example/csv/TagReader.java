package org.example.csv;

import org.apache.commons.csv.CSVRecord;
import org.example.model.TagEvent;

/**
 * CSV reader for tag events
 */
public class TagReader extends AbstractCSVReader<TagEvent> {

    @Override
    protected TagEvent parseCSVRecord(CSVRecord record) {
        try {
            String userId = getStringValue(record, "userId", true);
            String movieId = getStringValue(record, "movieId", true);
            String tag = getStringValue(record, "tag", true);
            Long timestamp = getLongValue(record, "timestamp", true);

            return new TagEvent(userId, movieId, tag, timestamp);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse CSV record: " + e.getMessage(), e);
        }
    }

    @Override
    protected String getEventTypeName() {
        return "tags";
    }
}
