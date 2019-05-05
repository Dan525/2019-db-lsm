package ru.mail.polis.pdaniil;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class SSTableFileChannel extends SSTable implements Table {

    private final Path file;
    private final int rowCount;
    private final long size;

    /** FileChannel.read() SSTable implementation.
     *
     * @param file directory of SSTable files
     * @throws IOException if unable to read SSTable files
     */
    public SSTableFileChannel(final Path file) throws IOException {

        this.file = file;

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {

            final ByteBuffer rowCountBuffer = ByteBuffer.allocate(Integer.BYTES);
            final long rowCountOff = channel.size() - Integer.BYTES;
            channel.read(rowCountBuffer, rowCountOff);
            rowCount = rowCountBuffer.rewind().getInt();
        }

        size = Files.size(file);

    }

    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new Iterator<>() {

            private final int lastIndex = rowCount - 1;

            private int position = findStartIndex(from, 0, lastIndex);

            @Override
            public boolean hasNext() {
                return position <= lastIndex;
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

    private long receiveOffset(final int index, final FileChannel channel) throws IOException {

        final ByteBuffer offsetBuffer = ByteBuffer.allocate(Long.BYTES);
        final long offsetOff = channel.size() - Integer.BYTES - Long.BYTES * (rowCount - index);
        channel.read(offsetBuffer, offsetOff);

        return offsetBuffer.rewind().getLong();
    }

    @Override
    protected ByteBuffer parseKey(final int index) throws IOException {

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {

            long offset = receiveOffset(index, channel);

            final ByteBuffer keySizeBuffer = ByteBuffer.allocate(Long.BYTES);
            channel.read(keySizeBuffer, offset);
            final long keySize = keySizeBuffer.rewind().getLong();

            offset += Long.BYTES;

            final ByteBuffer keyBuffer = ByteBuffer.allocate((int) keySize);
            channel.read(keyBuffer, offset);

            return keyBuffer.rewind();
        }
    }

    @Override
    protected Cell parseCell(final int index) throws IOException {

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {

            long offset = receiveOffset(index, channel);

            final ByteBuffer keySizeBuffer = ByteBuffer.allocate(Long.BYTES);
            channel.read(keySizeBuffer, offset);
            final long keySize = keySizeBuffer.rewind().getLong();

            offset += Long.BYTES;

            final ByteBuffer keyBuffer = ByteBuffer.allocate((int) keySize);
            channel.read(keyBuffer, offset);
            final ByteBuffer key = keyBuffer.rewind();

            offset += keySize;

            final ByteBuffer timeStampBuffer = ByteBuffer.allocate(Long.BYTES);
            channel.read(timeStampBuffer, offset);
            final long timeStamp = timeStampBuffer.rewind().getLong();

            offset += Long.BYTES;

            final ByteBuffer tombstoneBuffer = ByteBuffer.allocate(Byte.BYTES);
            channel.read(tombstoneBuffer, offset);
            final boolean tombstone = tombstoneBuffer.rewind().get() != 0;

            if (tombstone) {
                return Cell.create(key, Value.tombstone(timeStamp));
            } else {

                offset += Byte.BYTES;

                final ByteBuffer valueSizeBuffer = ByteBuffer.allocate(Long.BYTES);
                channel.read(valueSizeBuffer, offset);
                final long valueSize = valueSizeBuffer.rewind().getLong();

                offset += Long.BYTES;

                final ByteBuffer valueBuffer = ByteBuffer.allocate((int) valueSize);
                channel.read(valueBuffer, offset);
                final ByteBuffer value = valueBuffer.rewind();

                return Cell.create(key, Value.of(timeStamp, value));
            }
        }
    }

    public static Table flush(final Path tablesDir, final Iterator<Cell> cellIterator) throws IOException {
        return new SSTableFileChannel(writeTable(tablesDir, cellIterator));
    }

}
