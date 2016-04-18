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
    private static final int width = 28;
    private static final int height = 28;

    public static void main(String[] args) throws IOException {
        if (args.length < 1) System.exit(1);

        final String inputFolder = args[0];
        final String outputFolder = args[1];

        int count = 0;
        for (String f : new File(inputFolder).list()) {
            System.out.print(String.format("[%d] Converting %s to ", count++, f));
            Path in = Paths.get(inputFolder+"/"+f);
            byte[] raw = readSmallBinaryFile(in);
            Path out = Paths.get(outputFolder+"/"+f+".bmp");
            writeRawToBitmap(out, raw);
        }
    }
    private static void writeRawToBitmap(Path out, byte[] raw) throws IOException {
        int[] data = byteToInt(raw);
        BufferedImage outImage = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY);
        WritableRaster wr = outImage.getRaster();
        wr.setPixels(0, 0, width, height, data);
        outImage.setData(wr);
        ImageIO.write(outImage, "bmp", out.toFile());
        System.out.println(String.format("%s done", out.toFile()));
    }

    private static byte[] readSmallBinaryFile(Path aFileName) throws IOException {
        return Files.readAllBytes(aFileName);
    }

    private static int[] byteToInt(byte[] data) {
        int[] ints = new int[data.length];
        for (int i = 0; i < data.length; i++) {
            ints[i] = (int) data[i] & 0xff;
        }
        return ints;
    }

    private static void readBitmap() {
        //        BufferedImage img = ImageIO.read(new File(args[0]));
//        final int height = img.getHeight();
//        final int width = img.getWidth();
//        final int size = width * height;
//        int[] data = new int[size];
//        img.getData().getPixels(0, 0, width, height, data);
//        for (int i = 0; i < height; i++) {
//            for (int j = 0; j < width; j++) {
//                System.out.print(String.format("%d ", data[i * height + j]));
//            }
//            System.out.println();
//        }
    }
}
