package Project.analysis;

import java.io.File;

public class MasterAnalyzer {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java Project.analysis.MasterAnalyzer <execution_id>");
            return;
        }

        int executionId = Integer.parseInt(args[0]);
        System.out.println("======================================================================");
        System.out.println("                       MASTER ANALYSER TRIGGER                        ");
        System.out.println("======================================================================");
        System.out.println("Triggering analysis for Execution ID: " + executionId);

        // 1. Data Quality Analysis
        System.out.println("\n[STEP 1] Running Data Quality Analysis...");
        DataQualityAnalyzer.analyzeDataQuality(executionId);

        // Future Analyzers can be added here
        // QueryResultAnalyzer.analyze(executionId);
        
        System.out.println("\n======================================================================");
        System.out.println("                   ALL ANALYSES COMPLETED SUCCESSFULLY                ");
        System.out.println("======================================================================");
    }
}
