package reader;

import connect.DBConnection;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CSVReader {

    private static final Logger logger = Logger.getLogger(CSVReader.class.getName());

    public static void readCsv(Path file) {

            logger.info("Reading CSV file: " + file.getFileName());

            String sql = "INSERT INTO inventory (SKU, ProductName, Quantity, Price) VALUES (?, ?, ?, ?)";

            Connection conn = null;

            try{
                conn = DBConnection.getConnection();
                conn.setAutoCommit(false);

                try(BufferedReader br = Files.newBufferedReader(file);
                PreparedStatement pstmt = conn.prepareStatement(sql)) {


                String line = br.readLine();

                while((line = br.readLine()) != null) {
                        String[] values = line.split(",");
                        pstmt.setInt(1,Integer.parseInt(values[0]));
                        pstmt.setString(2, values[1]);
                        pstmt.setInt(3, Integer.parseInt(values[2]));
                        pstmt.setFloat(4, Float.parseFloat(values[3]));


                pstmt.addBatch();
                }

                pstmt.executeBatch();
                conn.commit();
                logger.info("Committed records of file:" + file.getFileName());
                processedFiles(file);


            }catch (Exception e) {
                e.printStackTrace();
                conn.rollback();
                logger.warning("Rollback performed for file:" + file.getFileName());
                moveErrorFiles(file);

            }}catch (Exception e) {
                logger.log(Level.SEVERE, "Error occurred while inserting records",e);

            }finally{

                try {

                    if(conn!=null){
                        conn.close();
                        logger.info("Connection closed for file:" + file.getFileName());
                    }}catch(SQLException e){
                        logger.log(Level.SEVERE, "Error occurred while closing connection",e);
                    }
                }
            }

            public static void moveErrorFiles(Path file) {
                Path errorDirPath = Paths.get("D:\\assignments\\feed v2\\litmus7\\inventoryfeed\\error");

                try {
                    Path target = errorDirPath.resolve(file.getFileName());
                    Files.move(file, target, StandardCopyOption.REPLACE_EXISTING);
                    logger.info("Moved error file: " + file.getFileName());
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Error occurred while moving error file", e);
                }

            }

            public static void processedFiles(Path file) {
                Path processedDirPath = Paths.get("D:\\assignments\\feed v2\\litmus7\\inventoryfeed\\processed");

                try {
                    Path target = processedDirPath.resolve(file.getFileName());
                    Files.move(file, target, StandardCopyOption.REPLACE_EXISTING);
                    logger.info("Moved processed file: " + file.getFileName());
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Error occurred while moving processed file", e);
                }
        }
    }


        
