package org.xudifsd.stored;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.xudifsd.stored.example.MemoryKeyValueStateMachine;
import org.xudifsd.stored.rpc.RPCHandler;
import org.xudifsd.stored.rpc.RPCServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.xudifsd.stored.example.MemoryKeyValueStateMachine.Op;
import static org.xudifsd.stored.example.MemoryKeyValueStateMachine.serializeOp;

public class RaftReactor {
    private static final Logger LOG = LoggerFactory.getLogger(RaftReactor.class);
    public static final int LEADER_HEARTBEAT_INTERVAL_MS = 1000;

    private String myId;

    private AtomicReference<RaftReactorState> state = new AtomicReference<RaftReactorState>(RaftReactorState.FOLLOWER);
    private ConcurrentLinkedQueue<StateObserver> observers = new ConcurrentLinkedQueue<StateObserver>();
    private InetSocketAddress[] members;
    private int serverPort;
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

    // TODO following five methods should be synchronized and should merged into one call
    public long getCurrentTerm() {
        return persist.getCurrentTerm();
    }

    public String getMyId() {
        return myId;
    }

    public long getPrevLogIndex() {
        // TODO implement this
        return 0;
    }

    public long getPrevLogTerm() {
        // TODO implement this
        return 0;
    }

    public long getLeaderCommit() {
        return commitIndex;
    }

    // user could use this method to do leader election
    public void registerObserver(StateObserver observer) {
        observers.add(observer);
    }

    public synchronized void changeStateWithPersist(long term, String votedFor, RaftReactorState state) throws IOException {
        persist.writeCurrentTerm(term);
        persist.writeVotedFor(votedFor);
        changeState(state);
    }

    public synchronized void changeState(RaftReactorState state) {
        this.state.set(state);
        for (StateObserver observer : observers) {
            observer.stateChanged(state);
        }
    }

    public void processArgs(String[] args) throws UnknownHostException {
        if (args.length < 2) {
            throw new IllegalArgumentException("must supply a path, port, and set of host:port tuples");
        }
        members = new InetSocketAddress[args.length - 2];
        for (int i = 2; i < args.length; ++i) {
            String[] r = args[i].split(":");
            if (r.length != 2) {
                throw new IllegalArgumentException("unknown host:port pair: " + args[i]);
            }
            members[i - 2] = new InetSocketAddress(InetAddress.getByName(r[0]), Integer.valueOf(r[1]));
        }

        myId = args[1];
        String[] r = myId.split(":");
        if (r.length != 2) {
            throw new IllegalArgumentException("unknown host:port pair for myId: " + myId);
        }
        serverPort = Integer.valueOf(r[1]);
    }

    public void run(String[] args) throws Exception {
        processArgs(args);

        stateMachine = new MemoryKeyValueStateMachine();

        persist = new Persist(args[0]);
        persist.restore(stateMachine);

        RPCServer server = new RPCServer(new RPCHandler(this), serverPort);
        proxy = new QuorumProxy(this, members);

        this.registerObserver(new StateLogger(getState()));
        this.registerObserver(proxy);

        executor = new ScheduledThreadPoolExecutor(1);
        executor.scheduleAtFixedRate(proxy, LEADER_HEARTBEAT_INTERVAL_MS,
                LEADER_HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

        // start
        LOG.info("should start from now on, myId is {}, members is {}", myId, Arrays.asList(members));

        server.start(1);
        Thread.sleep(3000); // without this, we can not shutdown properly

        // TODO call execute here
        List<ByteBuffer> in = new ArrayList<ByteBuffer>();
        List<ByteBuffer> out = new ArrayList<ByteBuffer>();
        in.add(ByteBuffer.wrap(serializeOp(Op.SET, "aaa", "bbb")));
        if (execute(in, out)) {
            LOG.debug("execute success");
        } else {
            LOG.debug("execute failed");
        }

        server.stop();
        executor.shutdown();
    }

    /**
    * Caller would block in this call, result is returned via out, if out is null,
    * it means caller does not care outcome. Returned value indicate this execution
    * is success or not, on returning false, out is not modified.
    * */
    public boolean execute(List<ByteBuffer> in, List<ByteBuffer> out) {
        if (state.get() == RaftReactorState.LEADER) {
            boolean result = proxy.commit(in);
            LOG.debug("proxy commit returns {}", result);
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
            LOG.warn("execute failed because this server is not leader");
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