package org.trend.spn.mnist;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by greghuang on 4/24/16.
 */
public abstract class MnistDbFile extends RandomAccessFile {

    private int count = 0;

    public MnistDbFile(String name, String mode) throws IOException {
        super(name, mode);

        if (getMagicNumber() != readInt()) {
            throw new RuntimeException("This MNIST DB file " + name + " should start with the number " + getMagicNumber() + ".");
        }

        count = readInt();
    }

    /**
     * MNIST DB files start with unique integer number.
     *
     * @return integer number that should be found in the beginning of the file.
     */
    protected abstract int getMagicNumber();

    public int getHeaderSize() {
        return 8; // two integers
    }

    /**
     * Number of bytes for each entry.
     * Defaults to 1.
     *
     * @return int
     */
    public int getEntryLength() {
        return 1;
    }

    public int getCount() {
        return count;
    }

    /**
     * The current entry index.
     *
     * @return long
     * @throws IOException
     */
    public long getCurrentIndex() throws IOException {
        return (getFilePointer() - getHeaderSize()) / getEntryLength() + 1;
    }

    /**
     * Set the required current entry index.
     *
     * @param curr
     *            the entry index
     */
    public void setCurrentIndex(long curr) {
        try {
            if (curr < 0 || curr > count) {
                throw new RuntimeException(curr + " is not in the range 0 to " + count);
            }
            seek(getHeaderSize() + (curr - 1) * getEntryLength());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Move to the next entry.
     *
     * @throws IOException
     */
    public void next() throws IOException {
        if (getCurrentIndex() < count) {
            skipBytes(getEntryLength());
        }
    }

    /**
     * Move to the previous entry.
     *
     * @throws IOException
     */
    public void prev() throws IOException {
        if (getCurrentIndex() > 0) {
            seek(getFilePointer() - getEntryLength());
        }
    }
}
