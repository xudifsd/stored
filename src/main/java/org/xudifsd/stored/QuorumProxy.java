package org.xudifsd.stored;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xudifsd.stored.rpc.RPCClient;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

public class QuorumProxy implements Runnable, StateObserver {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumProxy.class);

    private final InetSocketAddress[] members;
    private final RPCClient[] clients;

    public static final int CALLBACK_CAP = 100;
    public static final int APPEND_ENTRIES_TIMEOUT_MS = 1000;

    private AtomicReference<RaftReactorState> state;

    private List<ByteBuffer> entries;
    private int count = 0;
    private BlockingQueue<Boolean> callback;

    public QuorumProxy(InetSocketAddress[] addresses) {
        this.entries = new ArrayList<ByteBuffer>();
        this.callback = new ArrayBlockingQueue<Boolean>(CALLBACK_CAP);
        this.state = new AtomicReference<RaftReactorState>(RaftReactorState.FOLLOWER);
        this.members = addresses;
        this.clients = new RPCClient[members.length];

        for (int i = 0; i < members.length; ++i) {
            this.clients[i] = new RPCClient(members[i]);
        }
    }

    /*
    * Caller will block on this method, return true on more than
    * half of quorum stored entries persistently
    */
    public boolean commit(List<ByteBuffer> entries) {
        BlockingQueue<Boolean> callback;
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

        try {
            // used to modify CALLBACK_CAP to tune performance
            LOG.info("send {} of entries in batch", count);

            // TODO store persistently ourselves first

            if (state.get() == RaftReactorState.LEADER) {
                // TODO actually sent entries, and calculate if majority agreed
                // TODO use APPEND_ENTRIES_TIMEOUT_MS
            }
        } finally {
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
