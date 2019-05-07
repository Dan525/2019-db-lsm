package ru.mail.polis.pdaniil;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class MemTable implements Table {

    private final NavigableMap<ByteBuffer, Value> db = new TreeMap<>();
    private long size;    
    private final long version;

    /**
     * Implementation of in-memory table.
     * 
     * @param version version of current table
     */
    public MemTable(final long version) {
        this.version = version;
    }

    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {

        final Iterator<Map.Entry<ByteBuffer, Value>> entryIter = db.tailMap(from).entrySet().iterator();

        return Iterators.transform(entryIter, entry -> Cell.create(entry.getKey(), entry.getValue(), version));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final Value prev = db.put(key, Value.of(value));
        if (prev == null) {
            //added new key and value. calc space for them
            size += key.limit() + value.limit();
        } else if (prev.isRemoved()){
            //has only key before. Calc space for value
            size += value.limit();
        } else {
            //has key and value before. Calc prev and new value size difference
            size += value.limit() - prev.getData().limit();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final Value prev = db.put(key, Value.tombstone());
        if (prev == null) {
            //Calc key size
            size += key.limit();
        } else if (!prev.isRemoved()) {
            //substract prev value size
            size -= prev.getData().limit();
        }
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public long getVersion() {
        return version;
    }

}
