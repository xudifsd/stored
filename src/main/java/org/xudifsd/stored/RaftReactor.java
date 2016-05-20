package org.xudifsd.stored;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xudifsd.stored.rpc.RPCHandler;
import org.xudifsd.stored.rpc.RPCServer;

public class RaftReactor {
    private static final Logger LOG = LoggerFactory.getLogger(RaftReactor.class);
    public static final int serverPort = 12345;

    private RaftReactorState state = RaftReactorState.FOLLOWER;
    private Persist persist;

    private String[] members;

    // volatile state on all server
    private long commitIndex = 0;
    private long lastApplied = 0;

    // volatile state on leader
    private long[] nextIndex;
    private long[] matchIndex;

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
        RPCServer server = new RPCServer(new RPCHandler(), serverPort);
        server.start(1);
        server.stop();
    }

    public static void main(String[] args) throws Exception {
        RaftReactor reactor = new RaftReactor();
        reactor.run(args);
    }
}