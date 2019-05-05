package ru.mail.polis.pdaniil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

public abstract class SSTable {

    protected static final String TABLE_FILE_SUFFIX = ".dat";
    protected static final String TABLE_TMP_FILE_SUFFIX = ".tmp";

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
            } else {
                return mid;
            }
        }
        return curLow;
    }

    public static List<Table> findVersions(final Path tablesDir) throws IOException {
        final List<Table> ssTables = new ArrayList<>();
        Files.walkFileTree(tablesDir, EnumSet.noneOf(FileVisitOption.class), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                if (file.toString().endsWith(TABLE_FILE_SUFFIX)) {
                    ssTables.add(new SSTableFileChannel(file));
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return ssTables;
    }

    /** Writes SSTable in file.
     *
     * Each cell is sequentially written in the following format:
     * - keySize (8 bytes)
     * - key ("keySize" bytes)
     * - timestamp (8 bytes)
     * - tombstone (1 byte)
     *
     * If cell has value:
     * - valueSize (8 bytes)
     * - value ("valueSize" bytes)
     *
     * This is followed by offsets:
     * - offset (cellCount * 8 bytes)
     *
     * At the end of file is cell count:
     * - cellCount (4 bytes)
     *
     * @param tablesDir directory to write table
     * @param cellIterator iterator over cells, that you want to flush
     * @return path to the file in which the cells were written
     * @throws IOException if unable to open file
     */
    protected static Path writeTable(final Path tablesDir, final Iterator<Cell> cellIterator) throws IOException {

        final Path tmpFile = tablesDir.resolve(System.currentTimeMillis() + TABLE_TMP_FILE_SUFFIX);

        try (FileChannel channel = FileChannel.open(tmpFile,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW)) {

            final List<Integer> offsetList = new ArrayList<>();

            while (cellIterator.hasNext()) {

                offsetList.add((int) channel.position());

                final Cell cell = cellIterator.next();

                final long keySize = cell.getKey().limit();
                channel.write(ByteBuffer.allocate(Long.BYTES).putLong(keySize).flip());
                channel.write(ByteBuffer.allocate((int) keySize).put(cell.getKey()).flip());

                final long timeStamp = cell.getValue().getTimeStamp();
                channel.write(ByteBuffer.allocate(Long.BYTES).putLong(timeStamp).flip());

                final boolean tombstone = cell.getValue().isRemoved();
                channel.write(ByteBuffer.allocate(Byte.BYTES).put((byte) (tombstone ? 1 : 0)).flip());

                if (!tombstone) {
                    final ByteBuffer value = cell.getValue().getData();
                    final long valueSize = value.limit();
                    channel.write(ByteBuffer.allocate(Long.BYTES).putLong(valueSize).flip());
                    channel.write(ByteBuffer.allocate((int) valueSize).put(value).flip());
                }
            }

            final ByteBuffer offsetByteBuffer = ByteBuffer.allocate(Long.BYTES * offsetList.size());

            for (final int offset : offsetList) {
                offsetByteBuffer.putLong(offset);
            }

            channel.write(offsetByteBuffer.flip());

            channel.write(ByteBuffer.allocate(Integer.BYTES).putInt(offsetList.size()).flip());
        }

        final Path newTable = tablesDir.resolve(tmpFile.toString().replace(TABLE_TMP_FILE_SUFFIX, TABLE_FILE_SUFFIX));
        Files.move(tmpFile, newTable, StandardCopyOption.ATOMIC_MOVE);

        return newTable;
    }

    protected abstract ByteBuffer parseKey(final int index) throws IOException;

    protected abstract Cell parseCell(final int index) throws IOException;

}
