/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.mail.polis.pdaniil;

import java.nio.ByteBuffer;

/**
 *
 * @author daniil_pozdeev
 */
public class Cell implements Comparable<Cell> {
    
    private final ByteBuffer key;
    private final Value value;

    private Cell(ByteBuffer key, Value value) {
        this.key = key;
        this.value = value;
    }
    
    public static Cell create(ByteBuffer key, Value value) {
        return new Cell(key, value);
    }

    public ByteBuffer getKey() {
        return key;
    }

    public Value getValue() {
        return value;
    }

    @Override
    public int compareTo(Cell o) {
        if (this.getKey().compareTo(o.getKey()) == 0) {
            return this.getValue().compareTo(o.getValue());
        }
        return this.getKey().compareTo(o.getKey());
    }
    
}
