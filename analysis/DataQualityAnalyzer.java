package Project.analysis;

import Project.common.sql.DBConnection;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PiePlot;
import org.jfree.data.general.DefaultPieDataset;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DataQualityAnalyzer {

    private static final String GRAPH_DIR = "Project/analysis/graphs/DataQuality/";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java Project.analysis.DataQualityAnalyzer <execution_id>");
            return;
        }

        int executionId = Integer.parseInt(args[0]);

        // Ensure graph directory exists
        File dir = new File(GRAPH_DIR);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        analyzeDataQuality(executionId);
    }

    public static void analyzeDataQuality(int executionId) {
        System.out.println("======================================================================");
        System.out.println("                        DATA QUALITY PANEL                            ");
        System.out.println("======================================================================");

        long totalRecords = 0;
        long malformedRecords = 0;
        String datasetName = "";

        String sql = "SELECT dataset_name, SUM(batch_size) as total, SUM(malformed_record_count) as malformed " +
                     "FROM run_metadata WHERE execution_id = ? GROUP BY dataset_name";

        try (Connection conn = DBConnection.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setInt(1, executionId);
            ResultSet rs = pstmt.executeQuery();
            
            if (rs.next()) {
                datasetName = rs.getString("dataset_name");
                totalRecords = rs.getLong("total");
                malformedRecords = rs.getLong("malformed");
                
                long validRecords = totalRecords - malformedRecords;

                System.out.println("Execution ID:      " + executionId);
                System.out.println("Dataset:           " + datasetName);
                System.out.println("----------------------------------------------------------------------");
                System.out.println("Malformed Records: " + malformedRecords);
                System.out.println("Valid Records:     " + validRecords);
                System.out.println("Total Records:     " + totalRecords);
                System.out.println("----------------------------------------------------------------------");

                generateQualityPieChart(executionId, datasetName, validRecords, malformedRecords);
            } else {
                System.out.println("Error: Could not find execution_id " + executionId + " in the database.");
            }
        } catch (SQLException e) {
            System.err.println("Error fetching data quality stats: " + e.getMessage());
        }
        System.out.println("======================================================================");
    }

    private static void generateQualityPieChart(int executionId, String datasetName, long valid, long malformed) {
        DefaultPieDataset dataset = new DefaultPieDataset();
        dataset.setValue("Valid Records", valid);
        dataset.setValue("Malformed Records", malformed);

        JFreeChart chart = ChartFactory.createPieChart(
                "Data Quality Distribution - " + datasetName,
                dataset,
                true, true, false);

        applyModernPieTheme(chart);

        int width = 800;
        int height = 600;
        File chartFile = new File(GRAPH_DIR + "quality_chart_" + executionId + ".png");

        try {
            ChartUtils.saveChartAsPNG(chartFile, chart, width, height);
            System.out.println("-> Quality graph saved to: " + chartFile.getAbsolutePath());
        } catch (IOException e) {
            System.err.println("Error saving quality graph: " + e.getMessage());
        }
    }

    private static void applyModernPieTheme(JFreeChart chart) {
        chart.setBackgroundPaint(Color.WHITE);
        chart.getTitle().setFont(new Font("SansSerif", Font.BOLD, 22));

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setBackgroundPaint(Color.WHITE);
        plot.setOutlineVisible(false);
        plot.setShadowPaint(null); // No old-fashioned shadow

        // Vibrant Colors
        plot.setSectionPaint("Valid Records", new Color(46, 204, 113)); // Emerald Green
        plot.setSectionPaint("Malformed Records", new Color(231, 76, 60)); // Vibrant Red

        // Adjust labels
        plot.setLabelFont(new Font("SansSerif", Font.PLAIN, 14));
        plot.setLabelBackgroundPaint(new Color(240, 240, 240));
        plot.setLabelOutlinePaint(null);
        plot.setLabelShadowPaint(null);
        
        // Simple and clean legend
        chart.getLegend().setFrame(org.jfree.chart.block.BlockBorder.NONE);
        chart.getLegend().setItemFont(new Font("SansSerif", Font.PLAIN, 12));
    }
}
