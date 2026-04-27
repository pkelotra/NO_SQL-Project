package Project.common.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {
    // Database credentials - change these to match your local setup
    private static final String URL = "jdbc:postgresql://localhost:5432/NASA_db";
    private static final String USER = "postgres";
    private static final String PASSWORD = "postgres12"; // Update this with your actual password

    /**
     * Returns a connection to the PostgreSQL database.
     */
    public static Connection getConnection() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
            return DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            throw new SQLException("PostgreSQL JDBC Driver not found. Add it to your classpath.", e);
        }
    }
}
    