package Project.analysis;

import Project.common.sql.DBConnection;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.ui.RectangleInsets;
import org.jfree.data.category.DefaultCategoryDataset;

import java.awt.Color;
import java.awt.Font;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PipelineComparisonAnalyzer {

    private static final String GRAPH_DIR = "Project/analysis/graphs/PipelineComparison/";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java Project.analysis.PipelineComparisonAnalyzer <execution_id>");
            System.out.println("Example: java Project.analysis.PipelineComparisonAnalyzer 2");
            return;
        }

        int executionId = Integer.parseInt(args[0]);

        // Ensure graph directory exists
        File dir = new File(GRAPH_DIR);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        analyzePipelineComparison(executionId);
    }

    public static void analyzePipelineComparison(int executionId) {
        System.out.println("======================================================================");
        System.out.println("                    PIPELINE COMPARISON PANEL                         ");
        System.out.println("======================================================================");

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        String seriesName = "Runtime (seconds)";

        String datasetName = null;
        String currentPipeline = null;

        // 1. Get current execution details
        String currentSql = "SELECT dataset_name, pipeline_name, SUM(runtime) AS total_runtime " +
                            "FROM run_metadata WHERE execution_id = ? " +
                            "GROUP BY dataset_name, pipeline_name";

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(currentSql)) {
            
            pstmt.setInt(1, executionId);
            ResultSet rs = pstmt.executeQuery();
            
            if (rs.next()) {
                datasetName = rs.getString("dataset_name");
                currentPipeline = rs.getString("pipeline_name");
                double currentRuntime = rs.getDouble("total_runtime");
                
                System.out.printf("Target Execution: %d%n", executionId);
                System.out.printf("Dataset:          %s%n", datasetName);
                System.out.printf("Target Pipeline:  %s (Runtime: %.3f sec)%n", currentPipeline.toUpperCase(), currentRuntime);
                
                // Add to dataset
                dataset.addValue(currentRuntime, seriesName, currentPipeline.toUpperCase());
            } else {
                System.out.println("Error: Could not find execution_id " + executionId + " in the database.");
                return;
            }
        } catch (SQLException e) {
            System.err.println("Error fetching current execution details: " + e.getMessage());
            return;
        }

        // 2. Find fastest execution_id for OTHER pipelines on same dataset
        String getFastestSql = "WITH ExecutionStats AS (" +
                               "    SELECT execution_id, pipeline_name, SUM(runtime) AS total_runtime " +
                               "    FROM run_metadata " +
                               "    WHERE dataset_name = ? AND pipeline_name != ? " +
                               "    GROUP BY execution_id, pipeline_name " +
                               ") " +
                               "SELECT DISTINCT ON (pipeline_name) pipeline_name, execution_id, total_runtime " +
                               "FROM ExecutionStats ORDER BY pipeline_name, total_runtime ASC";

        System.out.println("----------------------------------------------------------------------");
        System.out.println("Finding fastest competitors on same dataset...");

        boolean competitorsFound = false;
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(getFastestSql)) {
            
            pstmt.setString(1, datasetName);
            pstmt.setString(2, currentPipeline);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                competitorsFound = true;
                String otherPipeline = rs.getString("pipeline_name");
                int otherExecId = rs.getInt("execution_id");
                double otherRuntime = rs.getDouble("total_runtime");
                
                System.out.printf("Competitor:       %s (Exec ID: %d, Runtime: %.3f sec)%n", 
                                  otherPipeline.toUpperCase(), otherExecId, otherRuntime);
                
                // Add to dataset
                dataset.addValue(otherRuntime, seriesName, otherPipeline.toUpperCase());
            }
        } catch (SQLException e) {
            System.err.println("Error fetching comparative execution data: " + e.getMessage());
            return;
        }

        if (!competitorsFound) {
            System.out.println("No comparative pipelines found for dataset: " + datasetName);
            // We still generate the graph, it will just have one bar
        }

        // 3. Generate the Bar Chart
        System.out.println("----------------------------------------------------------------------");
        System.out.println("Generating Bar Chart...");
        
        JFreeChart barChart = ChartFactory.createBarChart(
                "Pipeline Total Runtime Comparison - " + datasetName,
                "Pipeline",
                "Total Runtime (seconds)",
                dataset,
                PlotOrientation.VERTICAL,
                false, true, false); // No legend needed for single series

        applyModernThemeBarChart(barChart);

        int width = 800;
        int height = 600;
        File chartFile = new File(GRAPH_DIR + "runtime_comparison_" + executionId + ".png");

        try {
            ChartUtils.saveChartAsPNG(chartFile, barChart, width, height);
            System.out.println("-> Graph saved successfully to: " + chartFile.getAbsolutePath());
        } catch (IOException e) {
            System.err.println("Error saving graph: " + e.getMessage());
        }
        System.out.println("======================================================================");
    }

    private static void applyModernThemeBarChart(JFreeChart chart) {
        // Main Background
        chart.setBackgroundPaint(Color.WHITE);
        
        // Title Font
        chart.getTitle().setFont(new Font("SansSerif", Font.BOLD, 22));

        CategoryPlot plot = chart.getCategoryPlot();
        // Plot Background
        plot.setBackgroundPaint(new Color(250, 250, 250));
        
        // Gridlines
        plot.setRangeGridlinePaint(new Color(200, 200, 200));
        plot.setDomainGridlinesVisible(false); // No vertical gridlines
        
        // Plot Outline
        plot.setOutlineVisible(false);
        plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0));

        // Bar Styling
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        // Set a beautiful, vibrant neon pink/coral color to make it pop!
        renderer.setSeriesPaint(0, new Color(255, 51, 102)); 
        renderer.setDrawBarOutline(false);
        
        // Adjust Bar spacing
        renderer.setItemMargin(0.1);
        
        // Axis Fonts
        plot.getDomainAxis().setLabelFont(new Font("SansSerif", Font.BOLD, 14));
        plot.getDomainAxis().setTickLabelFont(new Font("SansSerif", Font.PLAIN, 12));
        plot.getRangeAxis().setLabelFont(new Font("SansSerif", Font.BOLD, 14));
        plot.getRangeAxis().setTickLabelFont(new Font("SansSerif", Font.PLAIN, 12));
    }
}
