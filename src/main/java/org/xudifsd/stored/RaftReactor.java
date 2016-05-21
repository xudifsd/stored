package org.xudifsd.stored;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xudifsd.stored.example.MemoryKeyValueStateMachine;
import org.xudifsd.stored.rpc.RPCHandler;
import org.xudifsd.stored.rpc.RPCServer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class RaftReactor {
    private static final Logger LOG = LoggerFactory.getLogger(RaftReactor.class);
    public static final int SERVER_PORT = 12345;
    public static final int LEADER_HEARTBEAT_INTERVAL_MS = 1000;

    private AtomicReference<RaftReactorState> state = new AtomicReference<RaftReactorState>(RaftReactorState.FOLLOWER);
    private String[] members;
    private ConcurrentLinkedQueue<StateObserver> observers = new ConcurrentLinkedQueue<StateObserver>();
    private QuorumProxy proxy;
    private ScheduledThreadPoolExecutor executor;
    private StateMachine stateMachine;

    private Persist persist;

    // volatile state on all server
    private long commitIndex = 0;
    private long lastApplied = 0;

    // volatile state on leader
    private long[] nextIndex;
    private long[] matchIndex;

    // user could use this method to do leader election
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

        stateMachine = new MemoryKeyValueStateMachine();

        persist = new Persist(args[0]);
        persist.restore(stateMachine);

        RPCServer server = new RPCServer(new RPCHandler(this), SERVER_PORT);
        proxy = new QuorumProxy();

        this.registerObserver(new StateLogger(getState()));
        this.registerObserver(proxy);

        executor = new ScheduledThreadPoolExecutor(1);
        executor.scheduleAtFixedRate(proxy, LEADER_HEARTBEAT_INTERVAL_MS,
                LEADER_HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

        // start
        LOG.info("should start from now on, members is {}", members);

        server.start(1);
        Thread.sleep(3000); // without this, we can not shutdown properly
        // TODO call execute here
        server.stop();
        executor.shutdown();
    }

    /*
    * Caller would block in this call, result is returned via out, if out is null,
    * it means caller does not care outcome. Returned value indicate this execution
    * is success or not, on returning false, out is not modified.
    * */
    public boolean execute(List<ByteBuffer> in, List<ByteBuffer> out) {
        if (state.get() == RaftReactorState.LEADER) {
            boolean result = proxy.commit(in);
            if (result) {
                // we do not persist in here, because we store it in proxy

                // FIXME what if stateMachine throw exception?
                List<ByteBuffer> stateMachineOut = stateMachine.apply(in);
                if (out != null) {
                    out.addAll(stateMachineOut);
                }
            }
            return result;
        } else {
            return false;
        }
    }

    public RaftReactorState getState() {
        return state.get();
    }

    public static void main(String[] args) throws Exception {
        RaftReactor reactor = new RaftReactor();
        reactor.run(args);
    }
}