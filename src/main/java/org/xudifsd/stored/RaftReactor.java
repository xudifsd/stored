package org.xudifsd.stored;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xudifsd.stored.rpc.RPCHandler;
import org.xudifsd.stored.rpc.RPCServer;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

public class RaftReactor {
    private static final Logger LOG = LoggerFactory.getLogger(RaftReactor.class);
    public static final int SERVER_PORT = 12345;
    public static final int LEADER_HEARTBEAT_INTERVAL_MS = 1000;

    private AtomicReference<RaftReactorState> state = new AtomicReference<RaftReactorState>(RaftReactorState.FOLLOWER);
    private String[] members;
    private ConcurrentLinkedQueue<StateObserver> observers = new ConcurrentLinkedQueue<StateObserver>();

    private Persist persist;

    // volatile state on all server
    private long commitIndex = 0;
    private long lastApplied = 0;

    // volatile state on leader
    private long[] nextIndex;
    private long[] matchIndex;

    public void registerObserver(StateObserver observer) {
        observers.add(observer);
    }

    public synchronized void changeState(long term, String votedFor, RaftReactorState state) throws IOException {
        persist.writeCurrentTerm(term);
        persist.writeVotedFor(votedFor);
        for (StateObserver observer : observers) {
            observer.stateChanged(state);
        }
    }

    public void run(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("must supply a path and set of host:port tuples");
        }
        members = new String[args.length - 1];
        for (int i = 1; i < args.length; ++i) {
            members[i - 1] = args[i];
        }
        persist = new Persist(args[0]);
        persist.restore();

        // start
        LOG.info("should start from now on, members is {}", members);
        RPCServer server = new RPCServer(new RPCHandler(this), SERVER_PORT);
        this.registerObserver(new StateLogger(getState()));

        server.start(1);
        Thread.sleep(3000); // without this, we can not shutdown properly
        server.stop();
    }

    public RaftReactorState getState() {
        return state.get();
    }

    public static void main(String[] args) throws Exception {
        RaftReactor reactor = new RaftReactor();
        reactor.run(args);
    }
}