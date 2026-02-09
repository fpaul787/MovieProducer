package org.example.csv;

import org.apache.commons.csv.CSVRecord;
import org.example.model.MovieEvent;

/**
 * CSV reader for movie events
 */
public class MovieReader extends AbstractCSVReader<MovieEvent> {

    @Override
    protected MovieEvent parseCSVRecord(CSVRecord record) {
        try {
            String movieId = getStringValue(record, "movieId", true);
            String title = getStringValue(record, "title", true);
            String genres = getStringValue(record, "genres", false);

            return new MovieEvent(movieId, title, genres);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse CSV record: " + e.getMessage(), e);
        }
    }

    @Override
    protected String getEventTypeName() {
        return "movie events";
    }
}
