package org.trend.spn;

/**
 * Created by greghuang on 4/19/16.
 */
public class ImgUtility {
    public static int[] byteToInt(byte[] data) {
        int[] ints = new int[data.length];
        for (int i = 0; i < data.length; i++) {
            ints[i] = (int) data[i] & 0xff;
        }
        return ints;
    }

    public static byte[] intToByte(int[] data) {
        byte[] bytes = new byte[data.length];

        for (int i = 0; i < data.length; i++) {
            bytes[i] = (byte) data[i];
        }
        return bytes;
    }

    public static int[] xFilter(int[] data, int w, int h) {
        int[] projected = new int[w];

        for (int j = 0; j < h; j++)
            for (int i = 0; i < w; i++) {
                if (data[j * w + i] != 0)
                    projected[i]++;
            }
        return projected;
    }

    public static int[] yFilter(int[] data, int w, int h) {
        int[] projected = new int[h];

        for (int i = 0; i < w; i++)
            for (int j = 0; j < h; j++) {
                if (data[j * w + i] != 0)
                    projected[j]++;
            }
        return projected;
    }
}
