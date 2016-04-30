package org.trend.spn.feature;

import org.apache.commons.lang3.ArrayUtils;
import org.trend.spn.BitmapLoader;
import org.trend.spn.ImgUtility;
import org.trend.spn.RawImgConvertor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Created by greghuang on 4/30/16.
 */
// For test
//        int[] test = new int[]{1,1,1,0,0,0,0,0,1};
//
//        int[] x = ImgUtility.xFilter(test, 3, 3);
//        int[] y = ImgUtility.yFilter(test, 3, 3);

public class ProjectFeature {
    public static void main(String[] args) throws IOException {
        if (args.length == 0 || args.length < 3) {
            System.out.println("Usage: bitmap_folder output_file label_file");
            System.exit(1);
        }

        final String inputFolder = args[0];
        final String outputFile = args[1];

        Path out = Paths.get(outputFile);
        if (out.toFile().exists()) out.toFile().delete();

        BufferedWriter bw = Files.newBufferedWriter(out);
        RawImgConvertor ric = new RawImgConvertor(args[2]);
        BitmapLoader bmpProcessor = new BitmapLoader(28, 28);
        int[] data = new int[28*28];

        for (String f : new File(inputFolder).list()) {
            if (!f.endsWith("bmp")) continue;

            Path in = Paths.get(inputFolder + "/" + f);
            bmpProcessor.readBitmap(in.toFile(), data);
            int[] x = ImgUtility.xFilter(data, 28, 28);
            int[] y = ImgUtility.yFilter(data, 28, 28);
            int[] result = (int[])ArrayUtils.addAll(x, y);
            String filename = f.substring(0, f.length()-4);
            ric.writeSingleTextFile(bw, ric.getLabel(filename), result, filename);
        }

        bw.close();

        System.out.println("Done");
    }
}