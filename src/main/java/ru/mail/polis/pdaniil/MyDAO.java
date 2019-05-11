package ru.mail.polis.pdaniil;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MyDAO implements DAO {

    private static final SSTable.Implementation SSTABLE_IMPL = SSTable.Implementation.FILE_CHANNEL_READ;
    private static final ByteBuffer MIN_BYTE_BUFFER = ByteBuffer.allocate(0);
    private static final double LOAD_FACTOR = 0.016;

    private final long allowableMemTableSize;

    private final Path tablesDir;
    
    private final AtomicLong versionCounter;

    private Table memTable;
    private List<Table> ssTableList;

    enum IterMode {

        VALUE_SEARCH(true),
        COMPACTION(false);

        private boolean includeMemTable;

        IterMode(final boolean includeMemTable) {
            this.includeMemTable = includeMemTable;
        }

        private boolean get() {
            return includeMemTable;
        }
    }

    /** 
     * DAO Implementation for LSM Database.
     *
     * @param tablesDir directory to store SSTable files
     * @param maxHeap max memory, allocated for JVM
     * @throws IOException if unable to read existing SSTable files
     */
    public MyDAO(final Path tablesDir, final long maxHeap) throws IOException {
        
        ssTableList = SSTable.findVersions(tablesDir, SSTABLE_IMPL);
        versionCounter = new AtomicLong(ssTableList.size());
        memTable = new MemTable(versionCounter.incrementAndGet());
        
        this.allowableMemTableSize = (long) (maxHeap * LOAD_FACTOR);
        this.tablesDir = tablesDir;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {

        return Iterators.transform(
                cellIterator(from, IterMode.VALUE_SEARCH),
                cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    private Iterator<Cell> cellIterator(
            @NotNull final ByteBuffer from,
            final IterMode includeMemTable) throws IOException {

        final List<Iterator<Cell>> ssIterators = new ArrayList<>();

        for (final Table ssTable : ssTableList) {
            ssIterators.add(ssTable.iterator(from));
        }

        if (includeMemTable.get()) {
            ssIterators.add(memTable.iterator(from));
        }

        final UnmodifiableIterator<Cell> mergeSortedIter =
                Iterators.mergeSorted(ssIterators, Comparator.naturalOrder());

        final Iterator<Cell> collapsedIter = Iters.collapseEquals(mergeSortedIter, Cell::getKey);

        return Iterators.filter(collapsedIter, cell -> !cell.getValue().isRemoved());
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

    private void flush() throws IOException {
        DebugUtils.flushInfo(memTable);

        ssTableList.add(SSTable.flush(
                tablesDir, 
                memTable.iterator(MIN_BYTE_BUFFER),
                memTable.getVersion(),
                SSTABLE_IMPL));

        memTable = new MemTable(versionCounter.incrementAndGet());
    }

    @Override
    public void compact() throws IOException {

        final Path actualFile = SSTable.writeTable(
                tablesDir,
                cellIterator(MIN_BYTE_BUFFER, IterMode.COMPACTION),
                memTable.getVersion());

        closeSSTables();
        SSTable.removeOldVersions(tablesDir, actualFile);
        ssTableList = SSTable.findVersions(tablesDir, SSTABLE_IMPL);

        assert ssTableList.size() == SSTable.MIN_TABLE_VERSION;
        versionCounter.set(SSTable.MIN_TABLE_VERSION);

        ((MemTable) memTable).setVersion(versionCounter.incrementAndGet());
    }

    private void closeSSTables() throws IOException {
        for (final Table t : ssTableList) {
            if (t instanceof SSTableFileChannel) {
                ((SSTableFileChannel) t).close();
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (memTable.getSize() != 0) {
            flush();
        }
        closeSSTables();
    }
}
