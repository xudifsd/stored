package org.xudifsd.stored;

public class Ballot implements Runnable {
    private final RaftReactor reactor;
    private final long term;

    public Ballot(RaftReactor reactor, long term) {
        this.reactor = reactor;
        this.term = term;
    }

    @Override
    public void run() {
        // TODO implement this, should broadcast RequestVote
    }
}
