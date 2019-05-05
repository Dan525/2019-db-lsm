package ru.mail.polis.pdaniil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public final class DebugUtils {

    private static final Logger log = LoggerFactory.getLogger(DebugUtils.class);
    private static final int MAX_BYTES_TO_SHOW = 30;

    private DebugUtils() {
    }

    /** Print in console bytes of two ByteBuffers to compare
     *
     * @param my bytebuffer to check
     * @param reference reference bytebuffer
     * @param tag info about comparison
     */
    public static void compareBytes(final ByteBuffer my, final ByteBuffer reference, final String tag) {
        log.info(String.format("%n~~~ %s%n", tag));

        final int myKeyShowLimit = my.limit() < MAX_BYTES_TO_SHOW ? my.limit() : MAX_BYTES_TO_SHOW;

        for (int i = 0; i < myKeyShowLimit; i++) {
            log.info(String.format(my.get(i) + (i == myKeyShowLimit - 1 ? "" : ", ") + "%n"));
        }

        final int refKeyShowLimit = my.limit() < MAX_BYTES_TO_SHOW ? my.limit() : MAX_BYTES_TO_SHOW;

        for (int i = 0; i < refKeyShowLimit; i++) {
            log.info(String.format(reference.get(i) + (i == refKeyShowLimit - 1 ? "" : ", ") + "%n%n"));
        }
    }

    /** Show in console info about cell:
     * - is cell removed;
     * - timestamp
     *
     * @param cell cell to get info about
     */
    public static void cellInfo(final Cell cell) {
        log.info(String.format("%nTombstone: %s%nTimestamp: %s%n%n",
                cell.getValue().isRemoved(),
                cell.getValue().getTimeStamp()));
    }

    /** Show in console info about flushing:
     * - Current memTable size;
     * - Heap free
     *
     * @param memTable memTable to flush
     */
    public static void flushInfo(final Table memTable) {
        log.info(String.format("%n==Flushing==%nCurrent memTable size: %d bytes%nHeap free: %d bytes%n%n",
                memTable.getSize(),
                Runtime.getRuntime().freeMemory()));
    }

}
