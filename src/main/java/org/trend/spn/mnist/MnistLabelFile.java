package org.trend.spn.mnist;

import java.io.IOException;

/**
 * Created by greghuang on 4/24/16.
 */
public class MnistLabelFile extends MnistDbFile {
    public MnistLabelFile(String path) throws IOException {
        super(path, "r");
    }

    @Override
    protected int getMagicNumber() {
        return 2049;
    }

    /**
     * Reads the integer at the current position.
     *
     * @return integer representing the label
     * @throws IOException
     */
    public int readLabel() throws IOException {
        return readUnsignedByte();
    }
}
