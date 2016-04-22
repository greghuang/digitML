import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by greghuang on 4/19/16.
 * Group files by each digit
 */
public class FileUtility {
    static Map<String, Integer> labelData;

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || args.length < 2) {
            System.out.println("Usage: input_folder output_folder [label_file]");
            System.exit(1);
        }

        if (args.length == 3) {
            final String labelFile = args[2];
            labelData = new HashMap<String, Integer>();
            loadLabelData(labelFile, labelData);
        }


        File inFolder = new File(args[0]);
        for (String f : inFolder.list()) {
            int label = labelData.get(f);
            if (createOutputFolder(args[1] + "/" + label)) {
                Path in = Paths.get(inFolder.getPath() + "/" + f);
                Path out = Paths.get(args[1] + "/" + label + "/" + f);
                if (!out.toFile().exists()) {
                    Files.copy(in, out);
                    System.out.println(String.format("Copy %s to %s done", in, out));
                }
            }
        }
    }

    private static boolean createOutputFolder(String outFolder) {
        File out = new File(outFolder);
        // Todo: Remvoe ones if exists

        // Create new one
        return out.isDirectory() || out.mkdir();
    }

    private static void loadLabelData(String fileName, Map<String, Integer> labelMap) {
        FileReader fr = null;
        try {
            fr = new FileReader(fileName);
            BufferedReader br = new BufferedReader(fr);
            boolean run = true;
            do {
                String line = br.readLine();
                if (line != null) {
                    String[] data = line.split(",");
                    labelMap.putIfAbsent(data[0].trim(), Integer.parseInt(data[1]));
                } else
                    run = false;
            } while (run);
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
