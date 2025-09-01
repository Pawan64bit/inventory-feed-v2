package Main;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import reader.CSVReader;

public class Main {

    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        Path inputFolder = Paths.get("D:\\assignments\\feed v2\\litmus7\\inventoryfeed\\input");

        ExecutorService executor = Executors.newCachedThreadPool();

        try (Stream<Path> paths = Files.list(inputFolder)) {
            paths.forEach(file -> executor.submit(() -> {

                logger.info("Processing file" + file.getFileName()+ "in thread" + Thread.currentThread().getName());

                CSVReader.readCsv(file);

                logger.info("Finished processing file" + file.getFileName()+ "in thread" + Thread.currentThread().getName());

            }));
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error listing files in input folder", e);
        } finally {
            executor.shutdown();
        }
    }
}
