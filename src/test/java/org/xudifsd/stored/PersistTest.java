package org.xudifsd.stored;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.xudifsd.stored.utils.Tuple;
import org.xudifsd.stored.utils.Utility;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PersistTest extends TestCase {

    private static class Helper {
        public static void assertByteArrayEqual(List<ByteBuffer> a, List<byte[]> b) {
            Assert.assertEquals(a.size(), b.size());
            for (int i = 0; i < a.size(); ++i) {
                byte[] aa = a.get(i).array();
                byte[] bb = b.get(i);
                Assert.assertEquals(aa.length, bb.length);
                for (int j = 0; j < aa.length; ++j) {
                    Assert.assertEquals(aa[j], bb[j]);
                }
            }
        }

        public static List<ByteBuffer> addTerms(List<ByteBuffer> entriesWithoutTerm) {
            byte[] term = Utility.longToBytes(1);
            List<ByteBuffer> result = new ArrayList<ByteBuffer>(entriesWithoutTerm.size());
            for (ByteBuffer buffer : entriesWithoutTerm) {
                byte[] buf = buffer.array();
                byte[] entry = new byte[term.length + buf.length];
                for (int i = 0; i < term.length; ++i) {
                    entry[i] = term[i];
                }
                for (int i = 0; i < buf.length; ++i) {
                    entry[term.length + i] = buf[i];
                }
                result.add(ByteBuffer.wrap(entry));
            }
            return result;
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

        p.init();

        List<ByteBuffer> inWithTerms = Helper.addTerms(in);
        p.commitLogEntries(0, inWithTerms);

        Tuple<List<byte[]>, List<Long>> result = p.readLogEntries(0, in.size());
        List<byte[]> out = result.t1;

        Helper.assertByteArrayEqual(in, out);

        // test read with offset
        result = p.readLogEntries(12, in.size() - 12);
        out = result.t1;
        Helper.assertByteArrayEqual(in.subList(12, in.size()), out);

        // test write with offset
        for (int i = 6; i < 9; ++i) {
            if (i - 6 < 3) {
                in.set(i, ByteBuffer.wrap(String.valueOf(i).getBytes()));
            }
        }
        while (in.size() > 9) {
            in.remove(9);
        }
        List<ByteBuffer> subWithTerms = Helper.addTerms(in.subList(6, in.size()));
        p.commitLogEntries(6, subWithTerms);
        try {
            // test shorter log entries would truncate log file
            p.readLogEntries(0, in.size() + 1);
            Assert.fail();
        } catch (EOFException e) {}
        result = p.readLogEntries(0, in.size());
        out = result.t1;
        Helper.assertByteArrayEqual(in, out);

        // corrupt check sum
        RandomAccessFile log = new RandomAccessFile(p.getLogFile(), "rw");
        log.seek(Integer.BYTES + Long.BYTES + in.get(0).array().length + 1);
        int v = log.read();
        log.seek(Integer.BYTES + Long.BYTES + in.get(0).array().length + 1);
        log.write(v + 1);
        try {
            p.readLogEntries(0, in.size());
            Assert.fail();
        } catch (IOException e) {
            "log file corrupted, detected by checksum".equals(e.getMessage());
        }
    }
}
