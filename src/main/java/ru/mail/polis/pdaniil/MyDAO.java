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

    private static final double LOAD_FACTOR = 0.016;

    private final long allowableMemTableSize;

    private final Path tablesDir;
    
    private final AtomicLong versionCounter;

    private Table memTable;
    private final List<Table> ssTableList;
    

    /** 
     * DAO Implementation for LSM Database.
     *
     * @param tablesDir directory to store SSTable files
     * @param maxHeap max memory, allocated for JVM
     * @throws IOException if unable to read existing SSTable files
     */
    public MyDAO(final Path tablesDir, final long maxHeap) throws IOException {
        
        ssTableList = SSTable.findVersions(tablesDir, SSTable.Implementation.FILE_CHANNEL_READ);
        versionCounter = new AtomicLong(ssTableList.size());
        memTable = new MemTable(versionCounter.incrementAndGet());
        
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

    private void flush() throws IOException {
        DebugUtils.flushInfo(memTable);

        ssTableList.add(SSTable.flush(
                tablesDir, 
                memTable.iterator(ByteBuffer.allocate(0)),
                memTable.getVersion(),
                SSTable.Implementation.FILE_CHANNEL_READ));

        memTable = new MemTable(versionCounter.incrementAndGet());
    }

    @Override
    public void close() throws IOException {
        if (memTable.getSize() != 0) {
            flush();
        }
        for (final Table t : ssTableList) {
            if (t instanceof SSTableFileChannel) {
                ((SSTableFileChannel) t).close();
            }

        }
    }
}
