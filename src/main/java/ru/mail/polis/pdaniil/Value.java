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
public class Value implements Comparable<Value> {

    private final long timeStamp;
    private final boolean tombstone;
    private final ByteBuffer value;

    private Value(long timeStamp, boolean tombstone, ByteBuffer value) {
        this.timeStamp = timeStamp;
        this.tombstone = tombstone;
        this.value = value;
    }

    public static Value of(ByteBuffer value) {
        return new Value(System.currentTimeMillis(), false, value);
    }

    public static Value of(long timeStamp, ByteBuffer value) {
        return new Value(timeStamp, false, value);
    }

    public static Value tombstone() {
        return new Value(System.currentTimeMillis(), true, null);
    }

    public static Value tombstone(long timeStamp) {
        return new Value(timeStamp, true, null);
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public boolean isRemoved() {
        return tombstone;
    }

    public ByteBuffer getValue() {
        return value;
    }

    @Override
    public int compareTo(Value o) {
        //"this" is lower if his timestamp is bigger
        return (int) (o.timeStamp - this.timeStamp);
    }

}
