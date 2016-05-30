package org.xudifsd.stored.utils;

import java.nio.ByteBuffer;

public class Utility {
    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    public static long parseTerm(byte[] data) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        for (int i = 0; i < Long.BYTES; ++i) {
            buffer.put(data[i]);
        }
        buffer.flip();//need flip
        return buffer.getLong();
    }
}
