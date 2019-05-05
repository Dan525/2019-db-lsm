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
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;

public class MyDAO implements DAO {

    private static final String TABLE_FILE_SUFFIX = ".dat";
    private static final String TABLE_TMP_FILE_SUFFIX = ".tmp";
    private static final double LOAD_FACTOR = 0.05;

    private final long allowableMemTableSize;

    private final Path tablesDir;

    private Table memTable;
    private final List<Table> ssTableList;

    public MyDAO(Path tablesDir, long maxHeap) throws IOException {
        memTable = new MemTable();
        ssTableList = findVersions(tablesDir);
        this.allowableMemTableSize = (long) (maxHeap * LOAD_FACTOR);
        this.tablesDir = tablesDir;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        List<Iterator<Cell>> ssIterators = ssTableList
                .stream()
                .map(t -> t.iterator(from))
                .collect(Collectors.toList());

        ssIterators.add(memTable.iterator(from));

        UnmodifiableIterator<Cell> mergeSortedIter = Iterators.mergeSorted(ssIterators, Comparator.naturalOrder());

        Iterator<Cell> collapsedIter = Iters.collapseEquals(mergeSortedIter, cell -> cell.getKey());

        UnmodifiableIterator<Cell> filteredCellIter =
                Iterators.filter(collapsedIter, cell -> !cell.getValue().isRemoved());

        return Iterators.transform(filteredCellIter, cell -> Record.of(cell.getKey(), cell.getValue().getValue()));
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate(), value.duplicate());
        if (memTable.getSize() > allowableMemTableSize) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        memTable.remove(key.duplicate());
        if (memTable.getSize() > allowableMemTableSize) {
            flush();
        }
    }

    private List<Table> findVersions(Path tablesDir) throws IOException {
        List<Table> ssTableList = new ArrayList<>();
        Files.walkFileTree(tablesDir, EnumSet.noneOf(FileVisitOption.class), 1, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (file.toString().endsWith(TABLE_FILE_SUFFIX)) {
                    ssTableList.add(new SSTable(file));
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return ssTableList;
    }

    private void flush() throws IOException {
        DebugUtils.flushInfo(memTable);

        Path tmpFile = tablesDir.resolve(String.valueOf(System.currentTimeMillis()) + TABLE_TMP_FILE_SUFFIX);

        try (FileChannel channel = FileChannel.open(tmpFile,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW)) {

            List<Integer> offsetList = new ArrayList<>();

            Iterator<Cell> cellIterator = memTable.iterator(ByteBuffer.allocate(0));

            while (cellIterator.hasNext()) {

                offsetList.add((int) channel.position());

                Cell cell = cellIterator.next();

                final long keySize = cell.getKey().limit();
                channel.write(ByteBuffer.allocate(Long.BYTES).putLong(keySize).flip());
                channel.write(ByteBuffer.allocate((int) keySize).put(cell.getKey()).flip());

                final long timeStamp = cell.getValue().getTimeStamp();
                channel.write(ByteBuffer.allocate(Long.BYTES).putLong(timeStamp).flip());

                final boolean tombstone = cell.getValue().isRemoved();
                channel.write(ByteBuffer.allocate(Byte.BYTES).put((byte) (tombstone ? 1 : 0)).flip());

                if (!tombstone) {
                    final ByteBuffer value = cell.getValue().getValue();
                    final long valueSize = value.limit();
                    channel.write(ByteBuffer.allocate(Long.BYTES).putLong(valueSize).flip());
                    channel.write(ByteBuffer.allocate((int) valueSize).put(value).flip());
                }
            }

            ByteBuffer offsetByteBuffer = ByteBuffer.allocate(Long.BYTES * offsetList.size());

            for (int offset : offsetList) {
                offsetByteBuffer.putLong(offset);
            }

            channel.write(offsetByteBuffer.flip());

            channel.write(ByteBuffer.allocate(Integer.BYTES).putInt(offsetList.size()).flip());
        }

        Path newTable = tablesDir.resolve(tmpFile.toString().replace(TABLE_TMP_FILE_SUFFIX, TABLE_FILE_SUFFIX));
        Files.move(tmpFile, newTable, StandardCopyOption.ATOMIC_MOVE);
        ssTableList.add(new SSTable(newTable));

        memTable = new MemTable();
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
