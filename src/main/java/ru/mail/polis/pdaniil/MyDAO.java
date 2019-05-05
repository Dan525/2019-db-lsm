package ru.mail.polis.pdaniil;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

public class MyDAO implements DAO {

    private static final String TABLE_FILE_SUFFIX = ".dat";
    private static final String TABLE_TMP_FILE_SUFFIX = ".tmp";
    private static final double LOAD_FACTOR = 0.05;

    private final long allowableMemTableSize;

    private final Path tablesDir;

    private Table memTable;
    private final List<Table> ssTableList;

    /** DAO Implementation for LSM Database
     *
     * @param tablesDir directory to store SSTable files
     * @param maxHeap max memory, allocated for JVM
     * @throws IOException if unable to read existing SSTable files
     */
    public MyDAO(final Path tablesDir, final long maxHeap) throws IOException {
        memTable = new MemTable();
        ssTableList = findVersions(tablesDir);
        this.allowableMemTableSize = (long) (maxHeap * LOAD_FACTOR);
        this.tablesDir = tablesDir;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {

        final List<Iterator<Cell>> ssIterators = new ArrayList<>();

        for (final Table ssTable : ssTableList) {
            ssIterators.add(ssTable.iterator(from));
        }

        ssIterators.add(memTable.iterator(from));

        final UnmodifiableIterator<Cell> mergeSortedIter =
                Iterators.mergeSorted(ssIterators, Comparator.naturalOrder());

        final Iterator<Cell> collapsedIter = Iters.collapseEquals(mergeSortedIter, cell -> cell.getKey());

        final UnmodifiableIterator<Cell> filteredCellIter =
                Iterators.filter(collapsedIter, cell -> !cell.getValue().isRemoved());

        return Iterators.transform(filteredCellIter, cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate(), value.duplicate());
        if (memTable.getSize() > allowableMemTableSize) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key.duplicate());
        if (memTable.getSize() > allowableMemTableSize) {
            flush();
        }
    }

    private List<Table> findVersions(final Path tablesDir) throws IOException {
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

    private void flush() throws IOException {
        DebugUtils.flushInfo(memTable);

        final Path tmpFile = tablesDir.resolve(System.currentTimeMillis() + TABLE_TMP_FILE_SUFFIX);

        try (FileChannel channel = FileChannel.open(tmpFile,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW)) {

            final List<Integer> offsetList = new ArrayList<>();

            final Iterator<Cell> cellIterator = memTable.iterator(ByteBuffer.allocate(0));

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
        ssTableList.add(new SSTableFileChannel(newTable));

        memTable = new MemTable();
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
