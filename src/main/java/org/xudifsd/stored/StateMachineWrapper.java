package org.xudifsd.stored;

import org.xudifsd.stored.utils.Utility;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class StateMachineWrapper implements StateMachine {
    private final StateMachine inner;
    private long commitIndex = 0;
    private long lastCommitTerm = 0;

    public StateMachineWrapper(StateMachine inner) {
        this.inner = inner;
    }

    @Override
    public List<ByteBuffer> apply(List<ByteBuffer> entriesWithTerm) {
        List<ByteBuffer> result = new ArrayList<ByteBuffer>(entriesWithTerm.size());
        List<ByteBuffer> in = new ArrayList<ByteBuffer>(1);

        for (ByteBuffer entryWithTerm : entriesWithTerm) {
            long term = Utility.parseTerm(entryWithTerm.array());
            if (term < lastCommitTerm) {
                throw new RuntimeException("invalid term");
            }
            lastCommitTerm = term;
            commitIndex += 1;
            byte[] entry = new byte[entryWithTerm.array().length - Long.BYTES];
            for (int i = Long.BYTES; i < entryWithTerm.array().length; ++i) {
                entry[i] = entryWithTerm.array()[i + Long.BYTES];
            }
            in.add(0, ByteBuffer.wrap(entry));
            result.addAll(inner.apply(in));
        }
        return null;
    }
}
