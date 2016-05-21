package org.xudifsd.stored.example;

import org.xudifsd.stored.StateMachine;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * return "" on non-exist key, key should not contains space & newline,
 * value should not contains newline
 * */
public class MemoryKeyValueStateMachine implements StateMachine {
    private static Charset charset = Charset.forName("UTF-8");
    private static byte[] getOpByte = "GET ".getBytes();
    private static byte[] setOpByte = "SET ".getBytes();
    private static byte[] deleteOpByte = "DELETE ".getBytes();
    private static byte[] space = " ".getBytes();
    private static byte[] succ = "succ ".getBytes();

    public enum Op {
        GET,
        SET,
        DELETE
    }

    private HashMap<String, String> map;

    // value could be null on GET
    public static byte[] serializeOp(Op op, String key, String value) {
        ByteBuffer result = null;
        if (key.contains(" ") || key.contains("\n")) {
            throw new IllegalArgumentException("key should not contain space or newline");
        }
        if (value != null && value.contains("\n")) {
            throw new IllegalArgumentException("value should not contain newline");
        }
        if (op == Op.GET) {
            byte[] b = key.getBytes(charset);
            result = ByteBuffer.allocate(getOpByte.length + b.length);
            result.put(getOpByte);
            result.put(b);
        } else if (op == Op.SET) {
            byte[] k = key.getBytes(charset);
            byte[] v = value.getBytes(charset);
            result = ByteBuffer.allocate(setOpByte.length + k.length + space.length + v.length);
            result.put(setOpByte);
            result.put(k);
            result.put(space);
            result.put(v);
        } else {
            byte[] b = key.getBytes();
            result = ByteBuffer.allocate(deleteOpByte.length + key.length());
            result.put(deleteOpByte);
            result.put(b);
        }
        return result.array();
    }

    public static Op deserializeOp(String op, String[] KV) {
        if (KV.length != 2) {
            throw new IllegalArgumentException("KV should has two slots");
        }
        String[] result = op.split(" ", 2);
        if (result.length != 2) {
            throw new IllegalArgumentException("illegal format");
        } // else {
        Op r;
        if (result[0].equals("GET")) {
            r = Op.GET;
            KV[0] = result[1];
        } else if (result[0].equals("SET")) {
            r = Op.SET;
            String[] result2 = result[1].split(" ", 2);
            if (result2.length != 2) {
                throw new IllegalArgumentException("illegal key value format: " + result[1]);
            } // else {
            KV[0] = result2[0];
            KV[1] = result2[1];
        } else if (result[0].equals("DELETE")) {
            r = Op.DELETE;
            KV[0] = result[1];
        } else {
            throw new IllegalArgumentException("unknown op " + result[0]);
        }
        return r;
    }

    // result can be null
    public static byte[] serializeResult(String result) {
        ByteBuffer buffer;
        if (result == null) {
            buffer = ByteBuffer.allocate(succ.length);
            buffer.put(succ);
        } else {
            byte[] resultByte = result.getBytes();
            buffer = ByteBuffer.allocate(succ.length + resultByte.length);
            buffer.put(succ);
            buffer.put(resultByte);
        }
        return buffer.array();
    }

    public static String deserializeResult(String result) {
        if (result.startsWith("succ ")) {
            return result.substring("succ ".length());
        } else {
            throw new IllegalArgumentException("unknown result " + result);
        }
    }

    public MemoryKeyValueStateMachine() {
        map = new HashMap<String, String>();
    }

    @Override
    public List<ByteBuffer> apply(List<ByteBuffer> entries) {
        List<ByteBuffer> result = new ArrayList<ByteBuffer>(entries.size());

        for (ByteBuffer entry : entries) {
            String[] KV = new String[2];
            Op op = deserializeOp(new String(entry.array(), charset), KV);
            ByteBuffer buffer;

            if (op == Op.GET) {
                String value = map.get(KV[0]);
                buffer = ByteBuffer.wrap(serializeResult(value));
            } else if (op == Op.SET) {
                map.put(KV[0], KV[1]);
                buffer = ByteBuffer.wrap(serializeResult(null));
            } else {
                map.remove(KV[0]);
                buffer = ByteBuffer.wrap(serializeResult(null));
            }
            result.add(buffer);
        }
        return result;
    }
}