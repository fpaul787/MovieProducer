package org.example.csv;

import org.apache.commons.csv.CSVRecord;
import org.example.model.LinkEvent;

public class LinkReader extends AbstractCSVReader<LinkEvent> {

    @Override
    protected LinkEvent parseCSVRecord(CSVRecord record) {
        try {
            String movieId = getStringValue(record, "movieId", true);
            String imdbId = getStringValue(record, "imdbId", true);
            String tmdbId = getStringValue(record, "tmdbId", true);

            return new LinkEvent(movieId, imdbId, tmdbId);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse CSV record: " + e.getMessage(), e);
        }
    }

    @Override
    protected String getEventTypeName() {
        return "link events";
    }

}
