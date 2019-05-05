package ru.mail.polis.pdaniil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

public interface Table {

    Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException;

    void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value);

    void remove(@NotNull ByteBuffer key);

    long getSize();
}
