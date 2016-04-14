import java.io.*;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by greghuang on 4/12/16.
 */
public class ImageConvertor {
    static StringBuilder sb = new StringBuilder();
    static Map<String, Integer> labelData;

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || args.length < 2) {
            System.out.println("Usage: training_file_folder output_file [label_file]");
            System.exit(1);
        }

        if (args.length == 3) {
            final String labelFile = args[2];
            labelData = new HashMap<String, Integer>();
            loadLabelData(labelFile, labelData);
        }

        final String outputFile = args[1];
        File dataFolder = null;
        BufferedWriter bw = null;
        FileWriter fw = null;

        try {
            dataFolder = new File(args[0]);
            String[] files = dataFolder.list();

            fw = new FileWriter(outputFile);
            bw = new BufferedWriter(fw);

            int cnt = 0;
            for (String file : files) {
                final String inFile = dataFolder.getPath() + "/" + file;
                int[] txtData = loadFile(inFile);
                if (txtData != null) {
                    //writeSingleTextFile(bw, file, txtData);
                    writeFileInLibsvm(bw, file, txtData);
                    cnt++;
                }
//                if (cnt == 2) break;
            }
            System.out.println(String.format("Write %d files to %s done", cnt, outputFile));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bw != null) bw.close();
        }
    }

    private static void writeFileInLibsvm(BufferedWriter bw, String fileName, int[] data) {
        if (bw != null) {
            final String sep = " ";
            sb.setLength(0);

            // insert label
            if (labelData != null) {
                int label = labelData.getOrDefault(fileName, -1);
                if (label == -1) System.err.println("No label for " + fileName);
                sb.append(label);
            }
            else throw new RuntimeException("No label data");

            for (int i = 0; i < data.length; i++) {
                if (data[i] != 0)
                    sb.append(sep).append(String.format("%d:%d", i+1, data[i]));
            }
            try {
                bw.write(sb.toString());
                bw.newLine();
                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void writeSingleTextFile(BufferedWriter bw, String fileName, int[] data) {
        if (bw != null) {
            final String sep = " ";
            sb.setLength(0);
            sb.append(fileName);

            // insert label
            if (labelData != null) {
                int label = labelData.getOrDefault(fileName, -1);
                if (label == -1) System.err.println("No label for " + fileName);
                sb.append(sep).append(label);
            }

            for (int i = 0; i < data.length; i++) {
                sb.append(sep).append(data[i]);
            }
            try {
                bw.write(sb.toString());
                bw.newLine();
                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void writeTextFile(String fileName, int[] data) throws IOException {
        BufferedWriter bw = null;
        FileWriter fw = null;
        try {
            fw = new FileWriter(fileName);
            bw = new BufferedWriter(fw);

            for (int i = 0; i < 28; i++) {
                for (int j = 0; j < 28; j++) {
                    int index = i * 28 + j;
                    sb.append(data[index]).append(" ");
                }
                sb.deleteCharAt(sb.length() - 1);
                bw.write(sb.toString());
                bw.newLine();
                sb.setLength(0);
            }
            bw.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bw != null) bw.close();
        }
    }

    private static int[] loadFile(String fileName) throws IOException {
        FileInputStream fin = null;
        try {
            fin = new FileInputStream(fileName);
            int[] data = new int[784];
            int c = -1;
            int i = 0;
            while ((c = fin.read()) != -1) {
                data[i++] = c;
            }
            return data;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fin != null) fin.close();
        }
        return null;
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
