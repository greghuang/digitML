package org.trend.spn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by greghuang on 4/27/16.
 * "20" "20" "data/train/spherizer_edge_20x20" "data/train/train_spherizer_edge_20x20.txt" "data/train.csv"
 * "20" "20" "data/train/ripple_edge_20x20" "data/train/train_ripple_edge_20x20.txt" "data/train.csv"
 * "20" "20" "data/train/twirl_sharp_20x20" "data/train/train_twirl_sharp_20x20.txt" "data/train.csv"
 * "20" "20" "data/train/resample_20_20" "data/train/train_resample_20x20.txt" "data/train.csv"
 * "20" "20" "data/train/pinch_edge_20x20" "data/train/train_pinch_edge_20x20.txt" "data/train.csv"
 * "20" "20" "data/train/zigzag_20_20" "data/train/train_zigzag_20x20.txt" "data/train.csv"
 * "20" "20" "data/test/testing_20x20" "data/test/testing_20x20_50000-1.txt"
 * "20" "20" "data/train/noise_20x20" "data/train/train_noise_20x20.txt" "data/train.csv"
 */
public class ProcessPipeline {
    public static void main(String[] args) {
        if (args.length == 0 || args.length < 4) {
            System.out.println("Usage: bitmap_folder output_file label_file width height");
            System.exit(1);
        }

        final int width = Integer.parseInt(args[0]);
        final int height = Integer.parseInt(args[1]);
        final String inputFolder = args[2];
        final String tmpRawFolder = "/tmp/"+System.currentTimeMillis();
        final String prefix = "";
        final String outputFile = args[3];
        final String labelFile = args.length == 5 ? args[4] : null;

        try {
            Path tmpFolder = Paths.get(tmpRawFolder);
            Files.createDirectory(tmpFolder);
            System.out.println("Save to " + tmpRawFolder);

            BitmapLoader bmpProcessor = new BitmapLoader(width, height);
            bmpProcessor.convertBitmapToRaw(inputFolder, tmpRawFolder);

            RawImgConvertor ric = new RawImgConvertor(labelFile);
            ric.mergeImages(tmpRawFolder, outputFile, false, prefix);

            //Files.delete(tmpFolder);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
