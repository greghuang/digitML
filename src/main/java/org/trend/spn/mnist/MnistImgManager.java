package org.trend.spn.mnist;

import org.trend.spn.RawImgConvertor;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by greghuang on 4/24/16.
 * "data/mnist/train-images.idx3-ubyte" "data/mnist/train-labels.idx1-ubyte" "data/mnist/testing_30000.txt"
 */
public class MnistImgManager {
    private MnistImageFile images;
    private MnistLabelFile labels;
    private String outPath;

    public MnistImgManager(String imgPath, String labelPath) throws IOException {
        if (imgPath != null)
            images = new MnistImageFile(imgPath);

        if (labelPath != null)
            labels = new MnistLabelFile(labelPath);
    }

    public int[] readImage(int index) throws IOException {
        if (images == null) {
            throw new IllegalStateException("Images file not initialized.");
        }
        images.setCurrentIdx(index);
        return images.readImage();
    }

    public int readLabel(int index) throws IOException {
        if (labels == null) {
            throw new IllegalStateException("labels file not initialized.");
        }
        labels.setCurrentIndex(index);
        return labels.readLabel();
    }

    public void writeImgToBitmap(File out, int[] image) throws IOException {
        int width = images.getRows();
        int height = images.getCols();
        if (outPath == null)
            throw new IllegalStateException("output path not initialized.");

        BufferedImage outImage = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY);
        WritableRaster wr = outImage.getRaster();
        wr.setPixels(0, 0, width, height, image);
        outImage.setData(wr);
        ImageIO.write(outImage, "bmp", out);
        System.out.println("Successful write image to " + out);
    }

    public void writeImgInLibsvm(BufferedWriter bw, int label, int[] image) {
        RawImgConvertor.writeFileInLibsvm(bw, label, image);
    }

    public void writeImgInFile(BufferedWriter bw, int label, int[] image, String filename) {
        RawImgConvertor.writeSingleTextFile(bw, label, image, filename);
    }

    public void writeLabelInFile(BufferedWriter bw, int label, String filename) {
        RawImgConvertor.writeLabeltoFile(bw, label, filename);
    }


//    public void showImage(int[] image) throws IOException {
//        BufferedWriter ppmOut = null;
//        try {
//            int rows = images.getRows();
//            int cols = images.getCols();
//
//            for (int i = 0; i < rows; i++) {
//                StringBuilder sb = new StringBuilder();
//                for (int j = 0; j < cols; j++) {
//                    sb.append(image[i][j] + " " + image[i][j] + " " + image[i][j] + "  ");
//                }
//                sb.toString();
//            }
//        } finally {
//            ppmOut.close();
//        }
//
//    }


    public static void main(String[] args) throws IOException {
        BufferedWriter bw = null;

        if (args.length < 3) System.exit(1);

        try {
            MnistImgManager manager = new MnistImgManager(args[0], args[1]);
//            String formatStr = "mnist%05d";
            String formatStr = "%d";

            Path out = Paths.get(args[2]);
            if (out.toFile().exists()) out.toFile().delete();

            bw = Files.newBufferedWriter(out);

            for (int i = 0; i < 60000; i++) {
                int[] image = manager.readImage(i);
                int label = manager.readLabel(i+1);
                String filename = String.format(formatStr, i);
                manager.writeLabelInFile(bw, label, filename);
//                manager.writeImgInFile(bw, label, image, String.format(formatStr, i));
//                manager.writeImgInLibsvm(bw, label, image);
//                manager.writeImgToBitmap(Paths.get(args[2] + "/" + i + ".bmp").toFile(), manager.readImage(i));
                System.out.println("Convert " + String.format(formatStr, i));
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            if (bw != null) bw.close();
        }
    }
}
