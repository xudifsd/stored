package org.xudifsd.stored.rpc;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xudifsd.stored.RaftReactor;
import org.xudifsd.stored.thrift.AppendEntriesResp;
import org.xudifsd.stored.thrift.RaftProtocol;
import org.xudifsd.stored.thrift.RequestVoteResp;

import java.nio.ByteBuffer;
import java.util.List;

public class RPCHandler implements RaftProtocol.Iface {
    private static final Logger LOG = LoggerFactory.getLogger(RPCHandler.class);

    private RaftReactor reactor;

    public RPCHandler(RaftReactor reactor) {
        this.reactor = reactor;
    }

    public AppendEntriesResp appendEntries(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                                           List<ByteBuffer> entries, long leaderCommit) throws TException {
        LOG.debug("received appendEntries, term {}, leaderId {}, prevLogIndex {}, prevLogTerm {}, leaderCommit {}",
                term, leaderId, prevLogIndex, prevLogTerm, leaderCommit);;
        return null;
    }

    public RequestVoteResp requestVote(long term, String candidateId, long lastLogIndex, long lastLogTerm) throws TException {
        LOG.debug("received appendEntries, term {}, candidateId {}, lastLogIndex {}, lastLogTerm {}",
                term, candidateId, lastLogIndex, lastLogTerm);
        return null;
    }
}
