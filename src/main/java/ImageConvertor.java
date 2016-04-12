import scala.Predef;

import java.io.*;
import java.util.Arrays;
import java.util.StringJoiner;

/**
 * Created by greghuang on 4/12/16.
 */
public class ImageConvertor {
    public static void main(String[] args) {
        if (args.length == 0 || args.length < 2) {
            System.out.println("No input data folder");
            System.exit(1);
        }

        File dataFolder = null;
        File outFolder = null;
        try {
            dataFolder = new File(args[0]);
            outFolder = new File(args[1]);
            String[] files = dataFolder.list();
            int cnt = 0;
            for (String file : files) {
                final String inFile = dataFolder.getPath() + "/" + file;
                final String outFile = outFolder.getPath() + "/" + file + ".txt";
                System.out.println("Input:" + inFile);
                System.out.println("Output:" + outFile);
                int[] txtData = loadFile(inFile);
                if (txtData != null) {
                    writeTextFile(outFile, txtData);
                    System.out.println(String.format("Convert %s to %s done", inFile, outFile));
                }
                cnt++;
                //if (cnt == 2) break;
            }
            System.out.println("Total files:" + cnt);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeTextFile(String fileName, int[] data) throws IOException {
        BufferedWriter bw = null;
        FileWriter fw = null;
        try {
            fw = new FileWriter(fileName);
            bw = new BufferedWriter(fw);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 28; i++) {
                for (int j = 0; j < 28; j++) {
                    int index = i * 28 + j;
                    sb.append(data[index]).append(" ");
                }
                sb.deleteCharAt(sb.length()-1);
                bw.write(sb.toString());
                bw.newLine();
                sb.setLength(0);
            }
            bw.flush();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
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
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (fin != null) fin.close();
        }
        return null;
    }

}
