package org.xudifsd.stored;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Scheduled periodically to see if we need reelection, start Ballot to
 * do actual broadcast, cancel existing one if exist.
 * */
public class ElectionStarter implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ElectionStarter.class);

    private final RaftReactor reactor;
    private final ScheduledExecutorService executor;
    private Future ballotFuture = null;

    public ElectionStarter(RaftReactor reactor, ScheduledExecutorService executor) {
        this.reactor = reactor;
        this.executor = executor;
    }

    @Override
    public void run() {
        if (reactor.getState() != RaftReactorState.LEADER) {
            long current = System.currentTimeMillis();
            long last = reactor.getLastValidAppendTime();
            if (current - last > RaftReactor.ELECTION_TIMEOUT_MS) {
                if (ballotFuture != null) {
                    ballotFuture.cancel(true);
                    ballotFuture = null;
                }
                LOG.info("starting a new election, last time we get valid heartbeat {}, current {}",
                        last, current);
                long nextTerm;
                try {
                    nextTerm = reactor.startNewElection();
                } catch (IOException e) {
                    LOG.error("get IOException while startNewElection", e);
                    return;
                }
                ballotFuture = executor.submit(new Ballot(reactor, nextTerm));
            }
        }
    }
}
