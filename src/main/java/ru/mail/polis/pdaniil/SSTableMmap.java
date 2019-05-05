package ru.mail.polis.pdaniil;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class SSTableMmap extends SSTable implements Table {

    private final int rowCount;
    private final LongBuffer offsetArray;
    private final ByteBuffer dataArray;
    private final long size;

    /** MMapped SSTable implementation
     *
     * @param file directory of SSTable files
     * @throws IOException if unable to read SSTable files
     */
    public SSTableMmap(final Path file) throws IOException {

        MappedByteBuffer mappped = null;

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            mappped = (MappedByteBuffer) channel
                    .map(FileChannel.MapMode.READ_ONLY, 0, channel.size())
                    .order(ByteOrder.BIG_ENDIAN);
        }

        final int rowCountOff = mappped.limit() - Integer.BYTES;

        final ByteBuffer rowCountDuplicate = mappped.duplicate();
        rowCountDuplicate.position(rowCountOff);
        rowCount = rowCountDuplicate.getInt();

        final int offsetArrayOff = mappped.limit() - Integer.BYTES - Long.BYTES * rowCount;

        final ByteBuffer offsetDuplicate = mappped.duplicate();
        offsetDuplicate.position(offsetArrayOff);
        offsetDuplicate.limit(rowCountOff);
        offsetArray = offsetDuplicate.slice().asLongBuffer();

        final ByteBuffer dataDuplicate = mappped.duplicate();
        dataDuplicate.limit(offsetArrayOff);
        dataArray = dataDuplicate.slice().asReadOnlyBuffer();

        size = Files.size(file);

    }

    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new Iterator<>() {

            private final int lastIndex = rowCount - 1;

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
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException("SSTableMmap is immutable");
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException("SSTableMmap is immutable");
    }

    @Override
    public long getSize() {
        return size;
    }

    private long receiveOffset(final int index) {
        return offsetArray.get(index);
    }

    private ByteBuffer parseKey(final long offset) {

        final ByteBuffer duplicate = dataArray.duplicate();
        duplicate.position((int) offset);
        final long keySize = duplicate.getLong();

        duplicate.limit((int) (duplicate.position() + keySize));

        return duplicate.slice();
    }

    public ByteBuffer parseKey(final int index) {
        return parseKey(receiveOffset(index));
    }

    private Cell parseCell(final long offset) {
        final ByteBuffer cellDuplicate = dataArray.duplicate();
        cellDuplicate.position((int) offset);
        cellDuplicate.slice();

        final ByteBuffer keyDuplicate = cellDuplicate.duplicate();
        final long keySize = keyDuplicate.getLong();

        keyDuplicate.limit((int) (keyDuplicate.position() + keySize));

        final int timeStampOff = keyDuplicate.limit();

        final ByteBuffer key = keyDuplicate.slice();

        final ByteBuffer valueDuplicate = cellDuplicate.duplicate();
        valueDuplicate.position(timeStampOff);
        final long timeStamp = valueDuplicate.getLong();
        final boolean tombstone = valueDuplicate.get() != 0;

        if (tombstone) {
            return Cell.create(key, Value.tombstone(timeStamp));
        } else {
            final long valueSize = valueDuplicate.getLong();
            valueDuplicate.limit((int) (valueDuplicate.position() + valueSize));
            final ByteBuffer value = valueDuplicate.slice();

            return Cell.create(key, Value.of(timeStamp, value));
        }
    }

    protected Cell parseCell(final int index) {
        return parseCell(receiveOffset(index));
    }

}
