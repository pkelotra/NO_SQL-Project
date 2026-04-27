package Project.common;
import java.io.*;

public class BatchSplitter {

    public static void splitFile(String inputFile, String pipelineName, int batchSize) {

        int batchId = 1;
        int lineCount = 0;

        BufferedReader reader = null;
        BufferedWriter writer = null;

        try {
            // 🔹 Create directory (MR / MongoDB / Pig / Hive)
            File dir = new File(pipelineName);
            if (!dir.exists()) {
                dir.mkdirs();
                System.out.println("Created directory: " + pipelineName);
            }

            reader = new BufferedReader(new FileReader(inputFile));

            String line;

            while ((line = reader.readLine()) != null) {

                // 🔹 Create new batch file when needed
                if (lineCount % batchSize == 0) {

                    if (writer != null) {
                        writer.close();
                    }

                    String batchFileName = pipelineName + "/batch_" + batchId + ".txt";
                    writer = new BufferedWriter(new FileWriter(batchFileName));

                    System.out.println("Creating: " + batchFileName);

                    batchId++;
                }

                writer.write(line);
                writer.newLine();

                lineCount++;
            }

            // 🔹 Close last writer
            if (writer != null) {
                writer.close();
            }

            System.out.println("\nTotal Records: " + lineCount);
            System.out.println("Total Batches: " + (batchId - 1));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) reader.close();
            } catch (IOException e) {}
        }
    }

    // 🔹 Main method
    public static void main(String[] args) {

        if (args.length != 3) {
            System.out.println("Usage: java BatchSplitter <input_file> <pipeline_name> <batch_size>");
            return;
        }

        String inputFile = args[0];
        String pipelineName = args[1];
        int batchSize = Integer.parseInt(args[2]);

        splitFile(inputFile, pipelineName, batchSize);
    }
}