package org.trend.spn.mnist;

import java.io.IOException;

/**
 * Created by greghuang on 4/24/16.
 */
public class MnistImageFile extends MnistDbFile {
    private int curIdx = 0;
    private int rows = 0;
    private int cols = 0;

    public MnistImageFile(String path) throws IOException {
        super(path, "r");

        rows = readInt();
        cols = readInt();
    }

    @Override
    protected int getMagicNumber() {
        return 2051;
    }

    @Override
    public int getEntryLength() {
        return cols * rows;
    }

    @Override
    public int getHeaderSize() {
        return super.getHeaderSize() + 8; // to more integers - rows and columns
    }

    /**
     * Number of rows per image.
     *
     * @return int
     */
    public int getRows() {
        return rows;
    }

    /**
     * Number of columns per image.
     *
     * @return int
     */
    public int getCols() {
        return cols;
    }

    public void setCurrentIdx(int index) {
        curIdx = index;
    }

    public int[] readImage() throws IOException {
        int[] dat = new int[getRows() * getCols()];
        for (int i = 0; i < getCols(); i++) {
            for (int j = 0; j < getRows(); j++) {
                dat[i * getRows() + j] = readUnsignedByte();
            }
        }
        return dat;
    }
}
