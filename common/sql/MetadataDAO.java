package Project.common.sql;

import java.sql.*;

public class MetadataDAO {

    /**
     * Inserts run metadata and returns the generated run_id.
     */
    /**
     * Inserts run metadata and returns the generated run_id.
     */
    /**
     * Gets the next execution_id for a new full pipeline run.
     */
    public static int getNextExecutionId() {
        String sql = "SELECT COALESCE(MAX(execution_id), 0) + 1 FROM run_metadata";
        try (Connection conn = DBConnection.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            System.err.println("Error getting next execution_id: " + e.getMessage());
        }
        return 1;
    }

    /**
     * Inserts run metadata and returns the generated run_id.
     */
    public static int insertRunMetadata(int executionId, String pipelineName, int batchId, int batchSize,
            double avgBatchSize, double runtime, int malformedCount, String datasetName) {
        String sql = "INSERT INTO run_metadata (execution_id, pipeline_name, batch_id, batch_size, avg_batch_size, runtime, malformed_record_count, dataset_name) "
                +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING run_id";

        try (Connection conn = DBConnection.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, executionId);
            pstmt.setString(2, pipelineName);
            pstmt.setInt(3, batchId);
            pstmt.setInt(4, batchSize);
            pstmt.setDouble(5, avgBatchSize);
            pstmt.setDouble(6, runtime);
            pstmt.setInt(7, malformedCount);
            pstmt.setString(8, datasetName);

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
