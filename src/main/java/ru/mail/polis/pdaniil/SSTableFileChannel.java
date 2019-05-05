package ru.mail.polis.pdaniil;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class SSTableFileChannel implements Table {

    private final Path file;
    private final int rowCount;
    private final long size;

    public SSTableFileChannel(Path file) throws IOException {

        this.file = file;

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {

            ByteBuffer rowCountBuffer = ByteBuffer.allocate(Integer.BYTES);
            final long rowCountOff = channel.size() - Integer.BYTES;
            channel.read(rowCountBuffer, rowCountOff);
            rowCount = rowCountBuffer.rewind().getInt();
        }

        size = Files.size(file);

    }

    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        return new Iterator<>() {

            private final int lastIndex = rowCount - 1;

            private int position = findStartIndex(from, 0, lastIndex);

            @Override
            public boolean hasNext() {
                return lastIndex >= position;
            }

            @Override
            public Cell next() {
                try {
                    return parseCell(position++);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
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

    private int findStartIndex(ByteBuffer from, int low, int high) throws IOException {
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

    private long receiveOffset(int index, FileChannel channel) throws IOException {

        ByteBuffer offsetBuffer = ByteBuffer.allocate(Long.BYTES);
        final long offsetOff = channel.size() - Integer.BYTES - Long.BYTES * (rowCount - index);
        channel.read(offsetBuffer, offsetOff);

        return offsetBuffer.rewind().getLong();
    }

    private ByteBuffer parseKey(int index) throws IOException {

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {

            long offset = receiveOffset(index, channel);

            ByteBuffer keySizeBuffer = ByteBuffer.allocate(Long.BYTES);
            channel.read(keySizeBuffer, offset);
            final long keySize = keySizeBuffer.rewind().getLong();

            offset += Long.BYTES;

            ByteBuffer keyBuffer = ByteBuffer.allocate((int) keySize);
            channel.read(keyBuffer, offset);

            return keyBuffer.rewind();
        }
    }

    private Cell parseCell(int index) throws IOException {

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {

            long offset = receiveOffset(index, channel);

            ByteBuffer keySizeBuffer = ByteBuffer.allocate(Long.BYTES);
            channel.read(keySizeBuffer, offset);
            final long keySize = keySizeBuffer.rewind().getLong();

            offset += Long.BYTES;

            ByteBuffer keyBuffer = ByteBuffer.allocate((int) keySize);
            channel.read(keyBuffer, offset);
            final ByteBuffer key = keyBuffer.rewind();

            offset += keySize;

            ByteBuffer timeStampBuffer = ByteBuffer.allocate(Long.BYTES);
            channel.read(timeStampBuffer, offset);
            final long timeStamp = timeStampBuffer.rewind().getLong();

            offset += Long.BYTES;

            ByteBuffer tombstoneBuffer = ByteBuffer.allocate(Byte.BYTES);
            channel.read(tombstoneBuffer, offset);
            final boolean tombstone = tombstoneBuffer.rewind().get() != 0;

            if (tombstone) {
                return Cell.create(key, Value.tombstone(timeStamp));
            } else {

                offset += Byte.BYTES;

                ByteBuffer valueSizeBuffer = ByteBuffer.allocate(Long.BYTES);
                channel.read(valueSizeBuffer, offset);
                final long valueSize = valueSizeBuffer.rewind().getLong();

                offset += Long.BYTES;

                ByteBuffer valueBuffer = ByteBuffer.allocate((int) valueSize);
                channel.read(valueBuffer, offset);
                final ByteBuffer value = valueBuffer.rewind();

                return Cell.create(key, Value.of(timeStamp, value));
            }
        }
    }

}
