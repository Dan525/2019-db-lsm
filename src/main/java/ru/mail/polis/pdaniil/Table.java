/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.mail.polis.pdaniil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

/**
 *
 * @author daniil_pozdeev
 */
public interface Table {

    public Iterator<Cell> iterator(@NotNull ByteBuffer from);

    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value);

    public void remove(@NotNull ByteBuffer key);

    public long getSize();
}
