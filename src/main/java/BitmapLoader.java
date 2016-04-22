import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;

/**
 * Created by greghuang on 4/18/16.
 */
public class BitmapLoader {
    private final int width;
    private final int height;

    public BitmapLoader(int w, int h) {
        width = w;
        height = h;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) System.exit(1);

        final String inputFolder = args[0];
        final String outputFolder = args[1];

        File out = new File(outputFolder);
        if (!out.exists()) out.mkdir();

        BitmapLoader bmploader = new BitmapLoader(28, 28);
//        bmploader.convertRawToBitmap(inputFolder, outputFolder);
        bmploader.convertBitmapToRaw(inputFolder, outputFolder);
    }

    public void convertBitmapToRaw(String inputFolder, String outputFolder) {
        int count = 0;
        int[] data = new int[width * height];

        for (String f : new File(inputFolder).list()) {
            try {
                if (!f.endsWith("bmp")) continue;
                System.out.print(String.format("[%d] Converting %s to ", count++, f));

                Path in = Paths.get(inputFolder + "/" + f);
                readBitmap(in.toFile(), data);
                Path out = Paths.get(outputFolder + "/" + f.substring(0, f.length() - 4));
                Files.write(out, ImgUtility.intToByte(data));
                System.out.println(out + " done");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void convertRawToBitmap(String inputFolder, String outputFolder) {
        int count = 0;
        for (String f : new File(inputFolder).list()) {
            try {
                System.out.print(String.format("[%d] Converting %s to ", count++, f));
                Path in = Paths.get(inputFolder + "/" + f);
                byte[] raw = readSmallBinaryFile(in);
                Path out = Paths.get(outputFolder + "/" + f + ".bmp");
                writeRawToBitmap(out, raw);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void writeRawToBitmap(Path out, byte[] raw) throws IOException {
        int[] data = ImgUtility.byteToInt(raw);
        BufferedImage outImage = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY);
        WritableRaster wr = outImage.getRaster();
        wr.setPixels(0, 0, width, height, data);
        outImage.setData(wr);
        ImageIO.write(outImage, "bmp", out.toFile());
        System.out.println(String.format("%s done", out.toFile()));
    }

    private byte[] readSmallBinaryFile(Path aFileName) throws IOException {
        return Files.readAllBytes(aFileName);
    }

    private static void readBitmap(File f, int[] buf) throws IOException {
        BufferedImage img = ImageIO.read(f);
        final int height = img.getHeight();
        final int width = img.getWidth();
        final int size = width * height;

        if (buf == null || buf.length != size)
            buf = new int[size];

        img.getData().getPixels(0, 0, width, height, buf);
//        for (int i = 0; i < height; i++) {
//            for (int j = 0; j < width; j++) {
//                System.out.print(String.format("%d ", buf[i * height + j]));
//            }
//            System.out.println();
//        }
    }
}
