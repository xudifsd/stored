package org.xudifsd.stored.example;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.xudifsd.stored.example.MemoryKeyValueStateMachine.Op;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static org.xudifsd.stored.example.MemoryKeyValueStateMachine.serializeOp;
import static org.xudifsd.stored.example.MemoryKeyValueStateMachine.deserializeOp;
import static org.xudifsd.stored.example.MemoryKeyValueStateMachine.serializeResult;
import static org.xudifsd.stored.example.MemoryKeyValueStateMachine.deserializeResult;

public class MemoryKeyValueStateMachineTest extends TestCase {
    private static Charset charset = Charset.forName("UTF-8");

    public MemoryKeyValueStateMachineTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(MemoryKeyValueStateMachineTest.class);
    }

    public void testSerializeDeserializeOp() {
        byte[] b = serializeOp(Op.GET, "aaa", null);
        String[] KV = new String[2];
        assertEquals(deserializeOp(new String(b, charset), KV), Op.GET);
        assertEquals(KV[0], "aaa");
        assertEquals(KV[1], null);

        b = serializeOp(Op.SET, "aaa", "bbb");
        KV = new String[2];
        assertEquals(deserializeOp(new String(b, charset), KV), Op.SET);
        assertEquals(KV[0], "aaa");
        assertEquals(KV[1], "bbb");

        // value could contains space
        b = serializeOp(Op.SET, "aaa", "bbb ccc");
        KV = new String[2];
        assertEquals(deserializeOp(new String(b, charset), KV), Op.SET);
        assertEquals(KV[0], "aaa");
        assertEquals(KV[1], "bbb ccc");

        b = serializeOp(Op.DELETE, "aaa", null);
        KV = new String[2];
        assertEquals(deserializeOp(new String(b, charset), KV), Op.DELETE);
        assertEquals(KV[0], "aaa");
        assertEquals(KV[1], null);

        b = serializeOp(Op.DELETE, "aaa", "dummy");
        KV = new String[2];
        assertEquals(deserializeOp(new String(b, charset), KV), Op.DELETE);
        assertEquals(KV[0], "aaa");
        assertEquals(KV[1], null);

        try {
            serializeOp(Op.GET, "aa aa", null);
            Assert.fail();
        } catch (IllegalArgumentException e) {}

        try {
            serializeOp(Op.GET, "aa\naa", null);
            Assert.fail();
        } catch (IllegalArgumentException e) {}

        try {
            serializeOp(Op.SET, "aaa", "aa\nbb");
            Assert.fail();
        } catch (IllegalArgumentException e) {}
    }

    public void testSerializeDeserializeResult() {
        assertEquals(deserializeResult(new String(serializeResult(null), charset)), "");
        assertEquals(deserializeResult(new String(serializeResult("aa"), charset)), "aa");
        assertEquals(deserializeResult(new String(serializeResult("aa bb"), charset)), "aa bb");
        assertEquals(deserializeResult(new String(serializeResult("a\nb"), charset)), "a\nb");
    }

    public void testStateMachine() {
        MemoryKeyValueStateMachine machine = new MemoryKeyValueStateMachine();

        List<ByteBuffer> ops = new ArrayList<ByteBuffer>();
        ops.add(ByteBuffer.wrap(serializeOp(Op.SET, "aaa", "bbb")));
        ops.add(ByteBuffer.wrap(serializeOp(Op.GET, "aaa", null)));
        ops.add(ByteBuffer.wrap(serializeOp(Op.DELETE, "aaa", null)));
        ops.add(ByteBuffer.wrap(serializeOp(Op.GET, "aaa", null)));
        ops.add(ByteBuffer.wrap(serializeOp(Op.SET, "bbb", "ccc")));
        ops.add(ByteBuffer.wrap(serializeOp(Op.SET, "bbb", "ddd")));
        ops.add(ByteBuffer.wrap(serializeOp(Op.GET, "bbb", null)));

        List<ByteBuffer> results = machine.apply(ops);
        assertEquals(results.size(), ops.size());
        assertEquals(deserializeResult(new String(results.get(0).array(), charset)), "");
        assertEquals(deserializeResult(new String(results.get(1).array(), charset)), "bbb");
        assertEquals(deserializeResult(new String(results.get(2).array(), charset)), "");
        assertEquals(deserializeResult(new String(results.get(3).array(), charset)), "");
        assertEquals(deserializeResult(new String(results.get(4).array(), charset)), "");
        assertEquals(deserializeResult(new String(results.get(5).array(), charset)), "");
        assertEquals(deserializeResult(new String(results.get(6).array(), charset)), "ddd");

        ops = new ArrayList<ByteBuffer>();
        ops.add(ByteBuffer.wrap(serializeOp(Op.GET, "bbb", null)));
        results = machine.apply(ops);

        assertEquals(results.size(), ops.size());
        assertEquals(deserializeResult(new String(results.get(0).array(), charset)), "ddd");
    }
}
