package org.xudifsd.stored;

import java.net.InetSocketAddress;

/**
 * This thread started by QuorumProxy once it find a straggler in Quorum,
 * and this thread is responsible to bring straggler up to date. Kill
 * itself once straggler returns success in AppendEntries, or current term
 * is not equal to returned one.
 * */
public class Spur implements Runnable {
    private final long term;
    private final InetSocketAddress straggler;
    private final Persist persist;
    private long lastLogIndex;

    public Spur(long term, InetSocketAddress straggler, Persist persist) {
        this.term = term;
        this.straggler = straggler;
        this.persist = persist;
    }

    @Override
    public void run() {
        // TODO implement this
    }
}
