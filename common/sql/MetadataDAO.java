package Project.common.sql;

import java.sql.*;

public class MetadataDAO {

    /**
     * Inserts run metadata and returns the generated run_id.
     */
    /**
     * Inserts run metadata and returns the generated run_id.
     */
    public static int insertRunMetadata(String pipelineName, int batchId, int batchSize,
            double avgBatchSize, double runtime, int malformedCount, String datasetName) {
        String sql = "INSERT INTO run_metadata (pipeline_name, batch_id, batch_size, avg_batch_size, runtime, malformed_record_count, dataset_name) "
                +
                "VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING run_id";

        try (Connection conn = DBConnection.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, pipelineName);
            pstmt.setInt(2, batchId);
            pstmt.setInt(3, batchSize);
            pstmt.setDouble(4, avgBatchSize);
            pstmt.setDouble(5, runtime);
            pstmt.setInt(6, malformedCount);
            pstmt.setString(7, datasetName);

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
     * Updates the runtime (adds to existing) and malformed count (takes max) for a specific run_id.
     */
    public static void updateFinalStats(int runId, double runtime, int malformedCount) {
        // We use runtime = runtime + ? to accumulate time for all queries
        // We use GREATEST to avoid tripling the malformed count (since all queries read the same file)
        String sql = "UPDATE run_metadata SET runtime = runtime + ?, malformed_record_count = GREATEST(malformed_record_count, ?) WHERE run_id = ?";

        try (Connection conn = DBConnection.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setDouble(1, runtime);
            pstmt.setInt(2, malformedCount);
            pstmt.setInt(3, runId);

            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error updating final stats: " + e.getMessage());
        }
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
