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

public class ProjectFeature {
    public static void main(String[] args) throws IOException {
        // For test
        int[] test = new int[]{1,1,1,0,0,0,0,0,1};

        int[] xx = ImgUtility.xFilter(test, 3, 3);
        int[] yy = ImgUtility.yFilter(test, 3, 3);

        float[] nx = normalize(xx);
        float[] ny = normalize(yy);

        System.exit(0);
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
            float[] result = (float[])ArrayUtils.addAll(normalize(x), normalize(y));
            String filename = f.substring(0, f.length()-4);
            ric.writeSingleTextFile(bw, ric.getLabel(filename), result, filename);
        }

        bw.close();

        System.out.println("Done");
    }

    public static float[] normalize(int[] data) {
        int max = 0;
        float[] norm = new float[data.length];

        for (int i = 0; i < data.length ; i++) {
            max = Math.max(max, data[i]);
        }
        for (int i = 0; i < data.length ; i++) {
            norm[i] = (float)data[i] / max;
        }
        return norm;
    }
}