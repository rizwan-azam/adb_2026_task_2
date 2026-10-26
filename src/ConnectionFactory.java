import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionFactory {
    public static final int POSTGRESQL = 1;

    public static Connection getDefaultParameterConnection(int type) {
        try {
            String url = "jdbc:postgresql://localhost:5432/blocking_db";
            Properties props = new Properties();
            props.setProperty("user", "postgres");
            props.setProperty("password", "write your password");
            return DriverManager.getConnection(url, props);
        } catch (SQLException e) {
            throw new RuntimeException("Could not connect to database", e);
        }
    }
}