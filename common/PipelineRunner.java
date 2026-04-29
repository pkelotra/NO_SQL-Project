package Project.common;

import java.io.*;
import java.util.*;
import Project.common.sql.MetadataDAO;

public class PipelineRunner {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String datasetPath = "Project/common/Dataset";
        File datasetDir = new File(datasetPath);

        if (!datasetDir.exists() || !datasetDir.isDirectory()) {
            System.out.println("Error: Dataset directory not found at " + datasetPath);
            return;
        }

        // 1. Select Folder
        File[] folders = datasetDir.listFiles(File::isDirectory);
        if (folders == null || folders.length == 0) {
            System.out.println("No batch folders found in " + datasetPath);
            return;
        }

        System.out.println("Available Batch Folders:");
        for (int i = 0; i < folders.length; i++) {
            System.out.println("[" + (i + 1) + "] " + folders[i].getName());
        }

        System.out.print("Select a folder (index): ");
        int folderIndex = scanner.nextInt() - 1;
        if (folderIndex < 0 || folderIndex >= folders.length) {
            System.out.println("Invalid selection.");
            return;
        }
        File selectedFolder = folders[folderIndex];

        // 2. Select Engine
        System.out.println("\nSelect Processing Engine:");
        System.out.println("[1] MongoDB");
        System.out.println("[2] MR (MapReduce)");
        System.out.print("Choice: ");
        int engineChoice = scanner.nextInt();
        String engineName = (engineChoice == 1) ? "MongoDB" : "MR";
        String classSuffix = (engineChoice == 1) ? "Mongo" : "MR";

        // 3. Compile Everything Once (Automatic)
        System.out.println("\n--- Running Maven Compile ---");
        runCommand("mvn compile");

        // 4. Load Batches
        File[] batchFiles = selectedFolder.listFiles((dir, name) -> name.endsWith(".txt") && !name.startsWith("temp_"));
        if (batchFiles == null || batchFiles.length == 0) {
            System.out.println("No batch files found in " + selectedFolder.getPath());
            return;
        }
        Arrays.sort(batchFiles, Comparator.comparing(File::getName));

        // 5. Calculate Stats
        long totalRecordsInFolder = 0;
        int nonEmptyBatches = batchFiles.length;
        int targetBatchSize = 0;
        try {
            String folderName = selectedFolder.getName();
            targetBatchSize = Integer.parseInt(folderName.substring(folderName.lastIndexOf('_') + 1));
        } catch (Exception e) {}

        for (File f : batchFiles) {
            String name = f.getName();
            try {
                int underscoreIndex = name.lastIndexOf('_');
                int dotIndex = name.lastIndexOf('.');
                totalRecordsInFolder += Integer.parseInt(name.substring(underscoreIndex + 1, dotIndex));
            } catch (Exception e) {}
        }
        double avgBatchSize = nonEmptyBatches > 0 ? (double) totalRecordsInFolder / nonEmptyBatches : 0;
        
        System.out.println("\nDataset Statistics:");
        System.out.println("Target Batch Size: " + targetBatchSize);
        System.out.println("Total Records: " + totalRecordsInFolder);
        System.out.println("Total Batches: " + nonEmptyBatches);
        System.out.printf("Average Batch Size: %.2f\n", avgBatchSize);

        System.out.println("\n--- Starting Pipeline Execution (All Queries) ---");
        int executionId = MetadataDAO.getNextExecutionId();
        System.out.println("Execution ID for this full run: " + executionId);

        for (File batchFile : batchFiles) {
            String fileName = batchFile.getName();
            System.out.println("\nProcessing Batch: " + fileName);

            int batchId = 0;
            int currentBatchSize = 0;
            try {
                int bIndex = fileName.indexOf('b');
                int underscoreIndex = fileName.lastIndexOf('_');
                int dotIndex = fileName.lastIndexOf('.');
                batchId = Integer.parseInt(fileName.substring(bIndex + 1, underscoreIndex));
                currentBatchSize = Integer.parseInt(fileName.substring(underscoreIndex + 1, dotIndex));
            } catch (Exception e) {}

            int runId = MetadataDAO.insertRunMetadata(executionId, engineName.toLowerCase(), batchId, currentBatchSize, avgBatchSize, 0, 0,
                    selectedFolder.getName());
            if (runId == -1) continue;

            System.out.println("Registered SQL Run ID: " + runId);

            // Execute All Queries (1, 2, and 3)
            for (int q = 1; q <= 3; q++) {
                runQuery(q, engineName, classSuffix, batchFile.getPath(), runId);
            }
        }
        System.out.println("\n--- Pipeline Execution Completed ---");
    }

    private static void runQuery(int queryNum, String engine, String suffix, String filePath, int runId) {
        if (engine.equalsIgnoreCase("MongoDB")) {
            runMongoQuery(queryNum, engine, suffix, filePath, runId);
        } else {
            runMRQuery(queryNum, filePath, runId);
        }
    }

    private static void runMongoQuery(int queryNum, String engine, String suffix, String filePath, int runId) {
        String mainClass = "Project.Query" + queryNum + "." + engine + ".Q" + queryNum + suffix;
        String command = "mvn exec:java -Dexec.mainClass=\"" + mainClass + "\" -Dexec.args=\"" + filePath + " " + runId + "\"";
        System.out.println("Executing Query " + queryNum + " (MongoDB)...");
        runCommand(command);
    }

    private static void runMRQuery(int queryNum, String inputPath, int runId) {
        String queryName = (queryNum == 1) ? "DailyTrafficMR" : (queryNum == 2) ? "TopResourcesMR" : "HourlyErrorMR";
        String mainClass = "Project.Query" + queryNum + ".MR." + queryName;

        File inputFile = new File(inputPath);
        String absInputPath = "file://" + (inputFile.getAbsolutePath().startsWith("/") ? "" : "/") + inputFile.getAbsolutePath();
        File outputFile = new File("Project/output_Q" + queryNum + "_run" + runId);
        String absOutputPath = "file://" + (outputFile.getAbsolutePath().startsWith("/") ? "" : "/") + outputFile.getAbsolutePath();
        String jarPath = "Project/Query" + queryNum + "/MR/" + queryName + ".jar";

        System.out.println("Executing Query " + queryNum + " (MapReduce)...");

        // 1. Compile Query
        runCommand("javac -classpath `hadoop classpath` -d . Project/common/Parsing/*.java Project/common/sql/*.java Project/Query" + queryNum + "/MR/" + queryName + ".java");

        // 2. Create Precise JAR
        runCommand("jar -cf " + jarPath + " Project/common/Parsing/*.class Project/common/sql/*.class Project/Query" + queryNum + "/MR/*.class");

        // 3. Run Hadoop
        runCommand("hadoop jar " + jarPath + " " + mainClass + " " + absInputPath + " " + absOutputPath + " " + runId);
    }

    private static void runCommand(String command) {
        try {
            Process process = new ProcessBuilder("bash", "-c", command).redirectErrorStream(true).start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                // Only print important Maven/Hadoop lines to keep output clean
                if (line.contains("[INFO] Building") || line.contains("BUILD SUCCESS") || line.contains("Step") || line.contains("Total Pipeline Runtime")) {
                    System.out.println(line);
                }
            }
            process.waitFor();
        } catch (Exception e) {}
    }
}

