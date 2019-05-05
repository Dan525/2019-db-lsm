/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.mail.polis.pdaniil;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

/**
 * @author daniil_pozdeev
 */
public class SSTableMmap implements Table {

    private final MappedByteBuffer mapped;

    private final int rowCount;
    private final LongBuffer offsetArray;
    private final ByteBuffer dataArray;
    private final long size;

    public SSTableMmap(Path file) throws IOException {

        MappedByteBuffer mappped = null;

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            mappped = (MappedByteBuffer) channel
                    .map(FileChannel.MapMode.READ_ONLY, 0, channel.size())
                    .order(ByteOrder.BIG_ENDIAN);
        }

        this.mapped = mappped;

        final int rowCountOff = mappped.limit() - Integer.BYTES;

        ByteBuffer rowCountDuplicate = mappped.duplicate();
        rowCountDuplicate.position(rowCountOff);
        rowCount = rowCountDuplicate.getInt();

        final int offsetArrayOff = mappped.limit() - Integer.BYTES - Long.BYTES * rowCount;

        ByteBuffer offsetDuplicate = mappped.duplicate();
        offsetDuplicate.position(offsetArrayOff);
        offsetDuplicate.limit(rowCountOff);
        offsetArray = offsetDuplicate.slice().asLongBuffer();

        ByteBuffer dataDuplicate = mappped.duplicate();
        dataDuplicate.limit(offsetArrayOff);
        dataArray = dataDuplicate.slice().asReadOnlyBuffer();

        size = Files.size(file);

    }

    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) {
        return new Iterator<>() {

            private final int lastIndex = offsetArray.limit() - 1;

            private int position = findStartIndex(from, 0, lastIndex);

            @Override
            public boolean hasNext() {
                return lastIndex >= position;
            }

            @Override
            public Cell next() {
                return parseCell(position++);
            }
        };
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) {
        throw new UnsupportedOperationException("SSTableMmap is immutable");
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        throw new UnsupportedOperationException("SSTableMmap is immutable");
    }

    @Override
    public long getSize() {
        return size;
    }

    private int findStartIndex(ByteBuffer from, int low, int high) {

        while (low <= high) {
            int mid = (low + high) / 2;

            ByteBuffer midKey = parseKey(mid);

            int compare = midKey.compareTo(from);

            if (compare < 0) {
                low = mid + 1;
            } else if (compare > 0) {
                high = mid - 1;
            } else if (compare == 0) {
                return mid;
            }
        }
        return low;
    }

    private long receiveOffset(int index) {
        return offsetArray.get(index);
    }

    private ByteBuffer parseKey(long offset) {

        ByteBuffer duplicate = dataArray.duplicate();
        duplicate.position((int) offset);
        long keySize = duplicate.getLong();

        duplicate.limit((int) (duplicate.position() + keySize));

        return duplicate.slice();
    }

    private ByteBuffer parseKey(int index) {
        return parseKey(receiveOffset(index));
    }

    private Cell parseCell(long offset) {
        ByteBuffer cellDuplicate = dataArray.duplicate();
        cellDuplicate.position((int) offset);
        cellDuplicate.slice();

        ByteBuffer keyDuplicate = cellDuplicate.duplicate();
        long keySize = keyDuplicate.getLong();

        keyDuplicate.limit((int) (keyDuplicate.position() + keySize));

        int timeStampOff = keyDuplicate.limit();

        ByteBuffer key = keyDuplicate.slice();

        ByteBuffer valueDuplicate = cellDuplicate.duplicate();
        valueDuplicate.position(timeStampOff);
        long timeStamp = valueDuplicate.getLong();
        boolean tombstone = valueDuplicate.get() != 0;

        if (tombstone) {
            return Cell.create(key, Value.tombstone(timeStamp));
        } else {
            long valueSize = valueDuplicate.getLong();
            valueDuplicate.limit((int) (valueDuplicate.position() + valueSize));
            ByteBuffer value = valueDuplicate.slice();

            return Cell.create(key, Value.of(timeStamp, value));
        }
    }

    private Cell parseCell(int index) {
        return parseCell(receiveOffset(index));
    }

}
