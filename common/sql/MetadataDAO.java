package Project.common.sql;

import java.sql.*;

public class MetadataDAO {

    /**
     * Inserts run metadata and returns the generated run_id.
     */
    public static int insertRunMetadata(String pipelineName, int batchId, int batchSize, 
                                       double avgBatchSize, double runtime, int malformedCount) {
        String sql = "INSERT INTO run_metadata (pipeline_name, batch_id, batch_size, avg_batch_size, runtime, malformed_record_count) " +
                     "VALUES (?, ?, ?, ?, ?, ?) RETURNING run_id";
        
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, pipelineName);
            pstmt.setInt(2, batchId);
            pstmt.setInt(3, batchSize);
            pstmt.setDouble(4, avgBatchSize);
            pstmt.setDouble(5, runtime);
            pstmt.setInt(6, malformedCount);
            
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            System.err.println("Error inserting run metadata: " + e.getMessage());
        }
        return -1;
    }

    /**
     * Updates the runtime for a specific run_id.
     */
    public static void updateRuntime(int runId, double runtime) {
        String sql = "UPDATE run_metadata SET runtime = ? WHERE run_id = ?";
        
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setDouble(1, runtime);
            pstmt.setInt(2, runId);
            
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error updating runtime: " + e.getMessage());
        }
    }
}
