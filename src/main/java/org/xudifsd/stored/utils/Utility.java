package org.xudifsd.stored.utils;

import java.nio.ByteBuffer;

public class Utility {
    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }
}
