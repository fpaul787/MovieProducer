/*
 * Movie Producer Application
 * Reads movie events from CSV, serializes them, and sends to Kafka
 */
package org.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.example.csv.LinkReader;
import org.example.csv.MovieReader;
import org.example.csv.RatingReader;
import org.example.csv.TagReader;
import org.example.model.LinkEvent;
import org.example.model.MovieEvent;
import org.example.model.RatingEvent;
import org.example.model.TagEvent;
import org.example.producer.LinkEventProducer;
import org.example.producer.MovieEventProducer;
import org.example.producer.RatingEventProducer;
import org.example.producer.TagEventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class App {
    
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    
    public static final Map<String, String> SMALL_CSVS = Map.of(
        "movies", "./ml_20m/movies_small.csv",
        "ratings", "./ml_20m/ratings_small.csv",
        "tags", "./ml_20m/tags_small.csv",
        "links", "./ml_20m/links_small.csv"
    );
    
    public static final Map<String, String> LARGE_CSVS = Map.of(
        "movies", "./ml_20m/movies.csv",
        "ratings", "./ml_20m/ratings.csv",
        "tags", "./ml_20m/tags.csv",
        "links", "./ml_20m/links.csv"
    );
    
    public static void main(String[] args) {
        logger.info("Starting Movie Producer Application...");
        
        try {
            // Read configuration
            final Properties config = readConfig("client.properties");

            // Run the main application logic
            run(config);
            
            logger.info("Movie Producer Application completed successfully");

        } catch (Exception e) {
            logger.error("Application failed with error: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    /**
     * Main application logic for reading CSV and initializing Kafka producer
     */
    private static void run(Properties config) throws Exception {
        run(config, true);
    }

    /**
     * Main application logic for reading CSV and initializing Kafka producer
     * @param useSmallCsvs if true, use small CSV files; otherwise use large CSV files
     */
    private static void run(Properties config, boolean useSmallCsvs) throws Exception {
        MovieEventProducer movieProducer = null;
        RatingEventProducer ratingProducer = null;
        TagEventProducer tagProducer = null;
        LinkEventProducer linkProducer = null;
        Map<String, String> csvPaths = useSmallCsvs ? SMALL_CSVS : LARGE_CSVS;

        try {
            // Read CSV and create MovieEvent objects
            List<MovieEvent> movieEvents = readMovieEvents(csvPaths.get("movies"));

            if (movieEvents.isEmpty()) {
                logger.warn("No movie events found in CSV file. Exiting.");
                return;
            }

            movieProducer = new MovieEventProducer(config);
            for (MovieEvent event : movieEvents) {
                movieProducer.sendEvent(event);
            }

            // Read ratings CSV
            List<RatingEvent> ratingEvents = readRatingEvents(csvPaths.get("ratings"));
            ratingProducer = new RatingEventProducer(config);
            for (RatingEvent event : ratingEvents) {
                ratingProducer.sendEvent(event);
            }

            // Read tags CSV
            List<TagEvent> tagEvents = readTagEvents(csvPaths.get("tags"));
            tagProducer = new TagEventProducer(config);
            for (TagEvent event : tagEvents) {
                tagProducer.sendEvent(event);
            }

            // Read links CSV
            List<LinkEvent> linkEvents = readLinkEvents(csvPaths.get("links"));
            linkProducer = new LinkEventProducer(config);
            for (LinkEvent event : linkEvents) {
                linkProducer.sendEvent(event);
            }
        } catch (IOException e) {
            logger.error("IO error occurred: {}", e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected error occurred: {}", e.getMessage(), e);
            throw e;
        } finally {
            if (movieProducer != null) {
                movieProducer.close();
            }
            if (ratingProducer != null) {
                ratingProducer.close();
            }
            if (tagProducer != null) {
                tagProducer.close();
            }
            if (linkProducer != null) {
                linkProducer.close();
            }
        }
    }

    /**
     * Reads configuration properties from a file
     * @param configFile the path to the configuration file
     * @return Properties object containing the configuration
     * @throws IOException if the file is not found or cannot be read
     */
    public static Properties readConfig(final String configFile) throws IOException {
    if (!Files.exists(Paths.get(configFile))) {
      throw new IOException(configFile + " not found.");
    }

    final Properties config = new Properties();
    try (InputStream inputStream = new FileInputStream(configFile)) {
      config.load(inputStream);
    }

    return config;
  }

  private static List<MovieEvent> readMovieEvents(String filePath) throws IOException {
      logger.info("Reading movie events from CSV file: {}", filePath);
      MovieReader csvReader = new MovieReader();
      return csvReader.readEvents(filePath);
  }

  private static List<RatingEvent> readRatingEvents(String filePath) throws IOException {
      logger.info("Reading rating events from CSV file: {}", filePath);
      RatingReader csvReader = new RatingReader();
      return csvReader.readEvents(filePath);
  }

  private static List<TagEvent> readTagEvents(String filePath) throws IOException {
      logger.info("Reading tag events from CSV file: {}", filePath);
      TagReader csvReader = new TagReader();
      return csvReader.readEvents(filePath);
  }

    private static List<LinkEvent> readLinkEvents(String filePath) throws IOException {
        logger.info("Reading link events from CSV file: {}", filePath);
        LinkReader csvReader = new LinkReader();
        return csvReader.readEvents(filePath);
    }
}
