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
import java.util.Properties;

import org.example.csv.MovieEventCSVReader;
import org.example.csv.RatingEventCSVReader;
import org.example.model.MovieEvent;
import org.example.model.RatingEvent;
import org.example.producer.MovieEventProducer;
import org.example.producer.RatingEventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class App {
    
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    
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
        MovieEventProducer movieProducer = null;
        RatingEventProducer ratingProducer = null;

        try {
            // Read CSV and create MovieEvent objects
            MovieEventCSVReader csvReader = new MovieEventCSVReader();
            String csvFilePath = "./ml_20m/movies.csv";

            logger.info("Reading movie events from CSV file: {}", csvFilePath);
            List<MovieEvent> movieEvents = csvReader.readMovieEvents(csvFilePath);

            if (movieEvents.isEmpty()) {
                logger.warn("No movie events found in CSV file. Exiting.");
                return;
            }

            logger.info("Successfully read {} movie events from CSV", movieEvents.size());

            // movieProducer = new MovieEventProducer(config);
            // for (MovieEvent event : movieEvents) {
            //     movieProducer.sendEvent(event);
            // }


            // Read ratings CSV
            RatingEventCSVReader ratingReader = new RatingEventCSVReader();
            String ratingsFilePath = "./ml_20m/ratings.csv";
            List<RatingEvent> ratingEvents = ratingReader.readRatingEvents(ratingsFilePath);

            logger.info("Successfully read {} rating events from CSV", ratingEvents.size());


            // ratingProducer = new RatingEventProducer(config);
            // for (RatingEvent event : ratingEvents) {
            //     ratingProducer.sendEvent(event);
            // }
        } finally {
            // Ensure producer is properly closed
            if (movieProducer != null) {
                movieProducer.close();
            }
            if (ratingProducer != null) {
                ratingProducer.close();
            }
        }
    }

    public static Properties readConfig(final String configFile) throws IOException {
    // reads the client configuration from client.properties
    // and returns it as a Properties object
    if (!Files.exists(Paths.get(configFile))) {
      throw new IOException(configFile + " not found.");
    }

    final Properties config = new Properties();
    try (InputStream inputStream = new FileInputStream(configFile)) {
      config.load(inputStream);
    }

    return config;
  }

}
