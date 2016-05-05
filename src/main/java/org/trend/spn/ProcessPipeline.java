package org.trend.spn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by greghuang on 4/27/16.
 * "data/train/Normalization_14x14" "data/train/featureSet/train_14_14_resample.txt" "data/train.csv"
 */
public class ProcessPipeline {
    public static void main(String[] args) {
        if (args.length == 0 || args.length < 2) {
            System.out.println("Usage: bitmap_folder output_file label_file");
            System.exit(1);
        }

        final String inputFolder = args[0];
        final String tmpRawFolder = "/tmp/"+System.currentTimeMillis();

        try {
            Path tmpFolder = Paths.get(tmpRawFolder);
            Files.createDirectory(tmpFolder);
            System.out.println("Save to " + tmpRawFolder);

            BitmapLoader bmpProcessor = new BitmapLoader(14, 14);
            bmpProcessor.convertBitmapToRaw(inputFolder, tmpRawFolder);

            RawImgConvertor ric = new RawImgConvertor(args[2]);
            ric.mergeImages(tmpRawFolder, args[1], false);

            //Files.delete(tmpFolder);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
