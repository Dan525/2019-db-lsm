package ru.mail.polis.pdaniil;

import java.nio.ByteBuffer;

public final class DebugUtils {

    private static final int MAX_BYTES_TO_SHOW = 30;

    private DebugUtils() {
    }

    /**
     * Print in console bytes of two ByteBuffers to compare
     *
     * @param my bytebuffer to check
     * @param reference reference bytebuffer
     * @param tag info about comparison
     */
    public static void compareBytes(final ByteBuffer my, final ByteBuffer reference, final String tag) {
        System.out.printf("~~~ %s\n", tag);

        final int myKeyShowLimit = my.limit() < MAX_BYTES_TO_SHOW ? my.limit() : MAX_BYTES_TO_SHOW;

        for (int i = 0; i < myKeyShowLimit; i++) {
            System.out.print(my.get(i) + (i == myKeyShowLimit - 1 ? "" : ", "));
        }

        System.out.println();

        final int refKeyShowLimit = my.limit() < MAX_BYTES_TO_SHOW ? my.limit() : MAX_BYTES_TO_SHOW;

        for (int i = 0; i < refKeyShowLimit; i++) {
            System.out.print(reference.get(i) + (i == refKeyShowLimit - 1 ? "" : ", "));
        }

        System.out.print("\n\n");
    }

    /**
     * Show in console info about cell:
     * - is cell removed;
     * - timestamp
     *
     * @param cell cell to get info about
     */
    public static void cellInfo(final Cell cell) {
        System.out.printf("Tombstone: %s\nTimestamp: %s\n\n",
                cell.getValue().isRemoved(),
                cell.getValue().getTimeStamp());
    }

    /**
     * Show in console info about flushing:
     * - Current memTable size;
     * - Heap free
     *
     * @param memTable memTable to flush
     */
    public static void flushInfo(final Table memTable) {
        System.out.printf("==Flushing==\nCurrent memTable size: %d bytes\nHeap free: %d bytes\n\n",
                memTable.getSize(),
                Runtime.getRuntime().freeMemory());
    }

}
