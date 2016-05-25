package org.xudifsd.stored;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xudifsd.stored.rpc.AppendEntriesResp;
import org.xudifsd.stored.rpc.RPCClient;
import org.xudifsd.stored.utils.Utility;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This thread is used in leader mode to persist newly created log and
 * broadcast AppendEntries to quorum, it only send logs start from last
 * AppendEntries call, and if it found any straggler it will start another
 * thread to handle the straggler.
 * */
public class QuorumProxy implements Runnable, StateObserver {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumProxy.class);

    public static final int CALLBACK_CAP = 100;
    public static final int APPEND_ENTRIES_TIMEOUT_MS = 1000;

    private final RaftReactor reactor;
    private final InetSocketAddress[] members;
    private final RPCClient[] clients;
    private final Future[] spurFuture; // will not start spur when previous spur is not done

    private AtomicReference<RaftReactorState> state;

    // entries should contains terms already
    private List<ByteBuffer> entries;
    private int count = 0;
    private BlockingQueue<Boolean> callback;

    public QuorumProxy(RaftReactor reactor, InetSocketAddress[] addresses) {
        this.reactor = reactor;
        this.entries = new ArrayList<ByteBuffer>();
        this.callback = new ArrayBlockingQueue<Boolean>(CALLBACK_CAP);
        this.state = new AtomicReference<RaftReactorState>(RaftReactorState.FOLLOWER);
        this.members = addresses;
        this.clients = new RPCClient[members.length];

        for (int i = 0; i < members.length; ++i) {
            this.clients[i] = new RPCClient(members[i]);
        }
        this.spurFuture = new Future[members.length];
    }

    /*
    * Caller will block on this method, return true on more than
    * half of quorum stored entries persistently
    */
    public boolean commit(List<ByteBuffer> entriesWithoutTerm, long currentTerm) {
        BlockingQueue<Boolean> callback;
        List<ByteBuffer> entries = new ArrayList<ByteBuffer>(entriesWithoutTerm.size());
        for (int i = 0; i < entriesWithoutTerm.size(); ++i) {
            ByteBuffer entry = entriesWithoutTerm.get(i);
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + entries.size());
            buffer.putLong(currentTerm);
            buffer.put(entry);

            buffer.reset();
            entry.reset();

            entries.add(buffer);
        }
        synchronized (this) {
            count += 1;
            /*
            * We have to ensure no other threads can add to entries, when we get whole
            * list swapped in run(), this can not simply be done via AtomicReference
            * */
            this.entries.addAll(entries);
            callback = this.callback;
        }
        try {
            return callback.take();
        } catch (InterruptedException e) {
            LOG.error("interrupted while take callback", e);
            return false;
        }
    }

    @Override
    public void run() {
        List<ByteBuffer> entries;
        int count;
        BlockingQueue<Boolean> callback;

        synchronized (this) {
            entries = this.entries;
            this.entries = new ArrayList<ByteBuffer>();

            count = this.count;
            this.count = 0;

            callback = this.callback;
            this.callback = new ArrayBlockingQueue<Boolean>(CALLBACK_CAP);
        }

        boolean result = false;
        int successCount = 0;

        try {
            // TODO store persistently ourselves first

            long timeout = APPEND_ENTRIES_TIMEOUT_MS;

            long start = System.currentTimeMillis();
            int waitingRespCount = clients.length;

            if (state.get() == RaftReactorState.LEADER) {
                // used to modify CALLBACK_CAP to tune performance
                LOG.info("send {} of entries in batch", count);

                BlockingQueue<AppendEntriesResp> resps;
                resps = new ArrayBlockingQueue<AppendEntriesResp>(clients.length);

                for (int i = 0; i < clients.length; ++i) {
                    clients[i].asyncAppendEntries(reactor.getCurrentTerm(), reactor.getMyId(),
                            reactor.getPrevLogIndex(), reactor.getPrevLogTerm(), entries, reactor.getLeaderCommit(),
                            resps);
                }
                while (waitingRespCount > 0 && timeout > 0) {
                    try {
                        AppendEntriesResp resp = resps.poll(timeout, TimeUnit.MILLISECONDS);
                        if (resp != null) {
                            waitingRespCount -= 1;
                        } else {
                            LOG.debug("time out while poll resps");
                        }
                        if (resp != null && resp.success) {
                            successCount += 1;
                        } else {
                            // TODO update currentTerm?
                        }
                    } catch (InterruptedException e) {
                        LOG.info("being interrupted during resps.poll", e);
                    }
                    timeout = APPEND_ENTRIES_TIMEOUT_MS - (System.currentTimeMillis() - start);
                }
            }
        } catch (TException e) {
            LOG.error("error in appendEntries", e);
        } catch (IOException e) {
            LOG.error("error in appendEntries", e);
        } finally {
            if (successCount * 2 >= clients.length) {
                // majority of clients agreed. If it's equal, we still have majority, including myself
                result = true;
            }
            LOG.debug("succ resp from {} clients, have {} members, result {}", successCount, clients.length, result);
            for (int i = 0; i < count; ++i) {
                callback.add(result);
            }
        }
    }

    @Override
    public void stateChanged(RaftReactorState state) {
        this.state.set(state);
    }
}
