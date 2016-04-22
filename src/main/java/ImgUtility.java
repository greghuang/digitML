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
            bytes[i] = (byte)data[i];
        }
        return bytes;
    }
}
