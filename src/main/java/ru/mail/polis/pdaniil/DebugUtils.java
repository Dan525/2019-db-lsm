package ru.mail.polis.pdaniil;

import java.nio.ByteBuffer;

public class DebugUtils {

    private static final int MAX_BYTES_TO_SHOW = 30;

    public static void compareBytes(ByteBuffer myKey, ByteBuffer referenceKey, String tag) {
        System.out.printf("~~~ %s\n", tag);

        int myKeyShowLimit = myKey.limit() < MAX_BYTES_TO_SHOW ? myKey.limit() : MAX_BYTES_TO_SHOW;

        for (int i = 0; i < myKeyShowLimit; i++) {
            System.out.print(myKey.get(i) + (i == myKeyShowLimit - 1 ? "" : ", "));
        }

        System.out.println();

        int refKeyShowLimit = myKey.limit() < MAX_BYTES_TO_SHOW ? myKey.limit() : MAX_BYTES_TO_SHOW;

        for (int i = 0; i < refKeyShowLimit; i++) {
            System.out.print(referenceKey.get(i) + (i == refKeyShowLimit - 1 ? "" : ", "));
        }

        System.out.print("\n\n");
    }

    public static void cellInfo(Cell cell) {
        System.out.printf("Tombstone: %s\nTimestamp: %s\n\n",
                cell.getValue().isRemoved(),
                cell.getValue().getTimeStamp());
    }

    public static void flushInfo(Table memTable) {
        System.out.printf("==Flushing==\nCurrent memTable size: %d bytes\nHeap free: %d bytes\n\n",
                memTable.getSize(),
                Runtime.getRuntime().freeMemory());
    }

}
