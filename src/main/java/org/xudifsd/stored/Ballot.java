package org.xudifsd.stored;

/**
 * This thread started by ElectionStarter, and only operate in only *one*
 * term, if ElectionStarter think we should start another election, it will
 * cancel any outstanding Ballot, which will interrupt any running Ballot
 * thread, so we should abort running.
 * */
public class Ballot implements Runnable {
    private final RaftReactor reactor;
    private final long term;

    public Ballot(RaftReactor reactor, long term) {
        this.reactor = reactor;
        this.term = term;
    }

    @Override
    public void run() {
        try {
            // TODO implement this, should broadcast RequestVote
            throw new InterruptedException();
        } catch (InterruptedException ex) {
            return;
        }
    }
}
