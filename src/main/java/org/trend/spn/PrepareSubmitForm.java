package org.trend.spn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by GregHuang on 5/6/16.
 * Run with "data/rf_csv" "data/mapping_csv" "data/submit.csv"
 */
public class PrepareSubmitForm {
    public static void main(String[] args) throws IOException {
        if (args.length < 3) System.exit(1);

        String resultPath = args[0];
        String mappingPath = args[1];
        String outputPath = args[2];

        Path result = Paths.get(resultPath + "/part-00000");
        Path mapping = Paths.get(mappingPath + "/part-00000");
        Path out = Paths.get(outputPath);

        Map<Integer, Integer> mappingTable = new HashMap<Integer, Integer>();
        List<String> lines = Files.readAllLines(mapping);
        lines.remove(0); // Remove header
        for (String line : lines) {
            String[] pair = line.split(",");
            // predictedLabel, predictionIndex
            int preLabel = (int) Double.parseDouble(pair[0]);
            int preIndex = (int) Double.parseDouble(pair[1]);
            mappingTable.putIfAbsent(preLabel, preIndex);
        }

        lines = Files.readAllLines(result);
        lines.remove(0);
        List<String> outBuff = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();

        for (String row : lines) {
            sb.setLength(0);
            String[] cols = row.split(",");
            if (cols.length != 13)
                throw new RuntimeException("row format is not expected, should be [name, predictedLabel, prediction, probability]");

            sb.append(cols[0]);

            // parse probility
            cols[3] = cols[3].substring(2, cols[3].length());
            cols[12] = cols[12].substring(0, cols[12].length()-2);
            List<String> probs = Arrays.asList(cols);
            probs = probs.subList(3, 13);
            for (int i = 0; i < probs.size(); i++) {
                sb.append(",").append(probs.get(mappingTable.get(i)));
            }
            //System.out.println(sb.toString());
            outBuff.add(sb.toString());
        }

        Files.write(out, outBuff);
    }
}
