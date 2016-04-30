package org.trend.spn;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by greghuang on 4/12/16.
 * <p>
 * libsvm: "data/train/all/" "data/output/training_all_libsvm.txt" "data/train.csv"
 */
public class RawImgConvertor {
    private static StringBuilder sb = new StringBuilder();
    private Map<String, Integer> labelData;

    public RawImgConvertor(String labelFile) {
        labelData = new HashMap<String, Integer>();
        loadLabelData(labelFile, labelData);
    }

    public void mergeImages(String inputFolder, String outputFile, boolean isLibsvm) throws IOException {
        Path in = Paths.get(inputFolder);
        String[] files = in.toFile().list();
        Path out = Paths.get(outputFile);
        if (out.toFile().exists()) out.toFile().delete();

        BufferedWriter bw = Files.newBufferedWriter(out);
        int cnt = 0;
        for (String file : files) {
            final String inFile = in + "/" + file;
            Path p = Paths.get(inFile);
            if (p.toFile().isHidden()) continue;

            System.out.println("Write "+inFile);

            int label = getLabel(file);
            byte[] raw = Files.readAllBytes(p);
            int[] txtData = ImgUtility.byteToInt(raw);
            if (txtData != null && label != -1) {
                if (isLibsvm)
                    writeFileInLibsvm(bw, label, txtData);
                else
                    writeSingleTextFile(bw, label, txtData);

                cnt++;
            }
//            break;
        }
        System.out.println(String.format("Write %d files to %s done", cnt, outputFile));
        bw.close();
    }


    public static void main(String[] args) throws IOException {
        if (args.length == 0 || args.length < 2) {
            System.out.println("Usage: training_file_folder output_file [label_file]");
            System.exit(1);
        }

        RawImgConvertor ric = new RawImgConvertor(args[2]);
        ric.mergeImages(args[0], args[1], false);
    }

    public int getLabel(String filename) {
        int label = -1;
        if (labelData != null) {
            label = labelData.getOrDefault(filename, -1);
            if (label == -1) System.err.println("No label for " + filename);
        } else throw new RuntimeException("No label data");
        return label;
    }

    public static void writeFileInLibsvm(BufferedWriter bw, int label, int[] data) {
        if (bw != null) {
            final String sep = " ";
            sb.setLength(0);

            // insert label
            sb.append(label);

            for (int i = 0; i < data.length; i++) {
                if (data[i] != 0)
                    sb.append(sep).append(String.format("%d:%d", i + 1, data[i]));
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

    public static void writeSingleTextFile(BufferedWriter bw, int label, int[] data) {
        writeSingleTextFile(bw, label, data, null);
    }

    public static void writeSingleTextFile(BufferedWriter bw, int label, int[] data, String filename) {
        if (bw != null) {
            final String sep = " ";
            sb.setLength(0);

            if (filename != null && !filename.equals(""))
                sb.append(filename).append(sep);

            // insert label
            sb.append(label);

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

    public void writeTextFile(String fileName, int[] data) throws IOException {
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

    private int[] loadFile(String fileName) throws IOException {
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

    private void loadLabelData(String fileName, Map<String, Integer> labelMap) {
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
