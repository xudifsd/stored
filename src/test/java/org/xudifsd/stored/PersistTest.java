package org.xudifsd.stored;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PersistTest extends TestCase {

    private static class Helper {
        public static void assertByteArrayEqual(List<byte[]> a, List<byte[]> b) {
            Assert.assertEquals(a.size(), b.size());
            for (int i = 0; i < a.size(); ++i) {
                byte[] aa = a.get(i);
                byte[] bb = b.get(i);
                Assert.assertEquals(aa.length, bb.length);
                for (int j = 0; j < aa.length; ++j) {
                    Assert.assertEquals(aa[j], bb[j]);
                }
            }
        }
    }

    public PersistTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(PersistTest.class);
    }

    public void testLogFile() throws IOException {
        List<ByteBuffer> in = new ArrayList<ByteBuffer>();

        in.add(ByteBuffer.wrap("True Colors".getBytes()));
        in.add(ByteBuffer.wrap("アンジェラ・アキ".getBytes()));
        in.add(ByteBuffer.wrap("".getBytes()));

        in.add(ByteBuffer.wrap("同じ色に染れない僕は独り いつも無口".getBytes()));
        in.add(ByteBuffer.wrap("果てない闇の深淵に立って".getBytes()));
        in.add(ByteBuffer.wrap("途方に暮れる時もある".getBytes()));
        in.add(ByteBuffer.wrap("Can you see my true colors shining through?".getBytes()));
        in.add(ByteBuffer.wrap("ありのままの 僕でいいのか".getBytes()));
        in.add(ByteBuffer.wrap("問いかけても 心の中".getBytes()));
        in.add(ByteBuffer.wrap("色あせた 自分の 声が".getBytes()));
        in.add(ByteBuffer.wrap("こだましている".getBytes()));
        in.add(ByteBuffer.wrap("".getBytes()));

        in.add(ByteBuffer.wrap("同じ色に染まらない君はなぜか いつも笑顔だ".getBytes()));
        in.add(ByteBuffer.wrap("痛みがわかる人の優しさで".getBytes()));
        in.add(ByteBuffer.wrap("僕にそっと つぶやいた".getBytes()));
        in.add(ByteBuffer.wrap("I see your true colors shining through".getBytes()));
        in.add(ByteBuffer.wrap("ありのままの 君が好きだよ".getBytes()));
        in.add(ByteBuffer.wrap("恐れずに 上を向いて".getBytes()));
        in.add(ByteBuffer.wrap("君だけの 可能性が".getBytes()));
        in.add(ByteBuffer.wrap("光っている".getBytes()));
        in.add(ByteBuffer.wrap("".getBytes()));

        in.add(ByteBuffer.wrap("like a rainbow, like a rainbow".getBytes()));
        in.add(ByteBuffer.wrap("".getBytes()));

        in.add(ByteBuffer.wrap("in a world full of people you can lose sight of it all".getBytes()));
        in.add(ByteBuffer.wrap("and the darkness inside you can make you feel so small".getBytes()));
        in.add(ByteBuffer.wrap("涙は未来の宝石だから".getBytes()));
        in.add(ByteBuffer.wrap("泣いたぶんだけ 価値がある".getBytes()));
        in.add(ByteBuffer.wrap("I see your true colors shining through".getBytes()));
        in.add(ByteBuffer.wrap("I see your true colors and that's why I love you".getBytes()));
        in.add(ByteBuffer.wrap("So don't be afraid to let them show".getBytes()));
        in.add(ByteBuffer.wrap("Your true colors your true colors are beautiful".getBytes()));
        in.add(ByteBuffer.wrap("I see your true colors shining through".getBytes()));
        in.add(ByteBuffer.wrap("ありのままの君でいいから".getBytes()));
        in.add(ByteBuffer.wrap("恐れずに 上を向いて".getBytes()));
        in.add(ByteBuffer.wrap("明日の 君だけの 可能性が 光っている".getBytes()));
        in.add(ByteBuffer.wrap("Like a rainbow".getBytes()));
        in.add(ByteBuffer.wrap("Like a rainbow".getBytes()));

        Persist p = new Persist(System.getProperty("java.io.tmpdir"));

        p.restore(new StateMachine() {
            @Override
            public List<ByteBuffer> apply(List<ByteBuffer> entries) {
                return new ArrayList<ByteBuffer>(entries.size());
            }
        });

        p.commitLogEntries(0, in);

        List<byte[]> out = p.readLogEntries(0, in.size());
        List<byte[]> inTransformed = new ArrayList<byte[]>(in.size());

        for (ByteBuffer buffer : in) {
            inTransformed.add(buffer.array());
        }
        Helper.assertByteArrayEqual(inTransformed, out);

        // test read with offset
        out = p.readLogEntries(12, in.size() - 12);
        inTransformed = new ArrayList<byte[]>(in.size() - 12);
        for (int i = 12; i < in.size(); ++i) {
            inTransformed.add(in.get(i).array());
        }
        Helper.assertByteArrayEqual(inTransformed, out);

        // test write with offset
        for (int i = 6; i < in.size(); ++i) {
            in.set(i, ByteBuffer.wrap(String.valueOf(i).getBytes()));
        }
        List<ByteBuffer> sub = in.subList(6, in.size());
        p.commitLogEntries(6, sub);
        inTransformed = new ArrayList<byte[]>(in.size());
        out = p.readLogEntries(0, in.size());
        for (ByteBuffer buffer : in) {
            inTransformed.add(buffer.array());
        }
        Helper.assertByteArrayEqual(inTransformed, out);
    }
}
