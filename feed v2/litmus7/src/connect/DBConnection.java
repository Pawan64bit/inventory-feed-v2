package connect;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBConnection {

    private static Connection connection = null;

    public static Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            try (InputStream input = DBConnection.class.getClassLoader().getResourceAsStream("prop.properties")) {

                Properties props = new Properties();
                props.load(input);

                String url = props.getProperty("db.url");
                String user = props.getProperty("db.username");
                String password = props.getProperty("db.password");

                connection = DriverManager.getConnection(url, user, password);

            } catch (Exception e) {
                throw new SQLException("Failed to load DB properties or establish connection", e);
            }
        }
        return connection;
    }
}
