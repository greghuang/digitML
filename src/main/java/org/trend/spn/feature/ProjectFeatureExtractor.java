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

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

/**
 * Created by greghuang on 4/30/16.
 * "data/train/Normalization_14x14" "data/train/features/projectFeature_14_14.txt" "data/train.csv"
 * "data/train/resample_20_20" "data/train/features/projectFeat_20x20.txt" "data/train.csv"
 */

public class ProjectFeatureExtractor {
    public static void main(String[] args) throws IOException {
        // For test
//        int[] test = new int[]{1,1,1,0,0,0,0,0,1};
//
//        int[] xx = ImgUtility.xFilter(test, 3, 3);
//        int[] yy = ImgUtility.yFilter(test, 3, 3);
//
//        float[] nx = normalize(xx);
//        float[] ny = normalize(yy);
//
//        System.exit(0);
        if (args.length == 0 || args.length < 3) {
            System.out.println("Usage: bitmap_folder output_file label_file");
            System.exit(1);
        }

        final int W = 20;
        final int H = 20;
        final String inputFolder = args[0];
        final String outputFile = args[1];
        final String label = args[2];

        extractFromRaw(W, H, inputFolder, outputFile, label);

        System.out.println("Done");
    }

    public static void extractFromRaw(int W, int H, String inputFolder, String outputFile, String label) throws IOException {
        RawImgConvertor ric = new RawImgConvertor(label);
        BitmapLoader bmpProcessor = new BitmapLoader(W, H);
        int[] data = new int[W*H];

        Path out = Paths.get(outputFile);
        if (out.toFile().exists()) out.toFile().delete();

        BufferedWriter bw = Files.newBufferedWriter(out, CREATE, APPEND);

        for (String f : new File(inputFolder).list()) {
            if (!f.endsWith("bmp")) continue;

            Path in = Paths.get(inputFolder + "/" + f);
            bmpProcessor.readBitmap(in.toFile(), data);
            int[] x = ImgUtility.xFilter(data, W, H);
            int[] y = ImgUtility.yFilter(data, W, H);
            float[] result = (float[])ArrayUtils.addAll(normalize(x), normalize(y));
            String filename = f.substring(0, f.length()-4);
            ric.writeSingleTextFile(bw, ric.getLabel(filename), result, filename);
        }

        bw.close();
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