package ru.mail.polis.pdaniil;

import java.nio.ByteBuffer;

public final class Cell implements Comparable<Cell> {
    
    private final ByteBuffer key;
    private final Value value;

    private Cell(final ByteBuffer key, final Value value) {
        this.key = key;
        this.value = value;
    }
    
    public static Cell create(final ByteBuffer key, final Value value) {
        return new Cell(key, value);
    }

    public ByteBuffer getKey() {
        return key;
    }

    public Value getValue() {
        return value;
    }

    @Override
    public int compareTo(final Cell o) {
        final int compareKey = this.getKey().compareTo(o.getKey());
        if (compareKey == 0) {
            return this.getValue().compareTo(o.getValue());
        }
        return compareKey;
    }
    
}
