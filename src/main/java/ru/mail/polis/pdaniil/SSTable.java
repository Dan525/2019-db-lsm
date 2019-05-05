package ru.mail.polis.pdaniil;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class SSTable {

    protected int findStartIndex(final ByteBuffer from, final int low, final int high) throws IOException {

        int curLow = low;
        int curHigh = high;

        while (curLow <= curHigh) {
            final int mid = (curLow + curHigh) / 2;

            final ByteBuffer midKey = parseKey(mid);

            final int compare = midKey.compareTo(from);

            if (compare < 0) {
                curLow = mid + 1;
            } else if (compare > 0) {
                curHigh = mid - 1;
            } else if (compare == 0) {
                return mid;
            }
        }
        return curLow;
    }

    protected abstract ByteBuffer parseKey(final int index) throws IOException;

    protected abstract Cell parseCell(final int index) throws IOException;

}
