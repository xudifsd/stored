package org.xudifsd.stored.rpc;

import org.apache.thrift.TException;
import org.xudifsd.stored.RaftReactor;
import org.xudifsd.stored.thrift.AppendEntriesResp;
import org.xudifsd.stored.thrift.RaftProtocol;
import org.xudifsd.stored.thrift.RequestVoteResp;

import java.nio.ByteBuffer;
import java.util.List;

public class RPCHandler implements RaftProtocol.Iface {
    private RaftReactor reactor;

    public RPCHandler(RaftReactor reactor) {
        this.reactor = reactor;
    }

    public AppendEntriesResp appendEntries(long term, String leaderId, long prevLogIndex, long prevLogTerm, List<ByteBuffer> entries, long leaderCommit) throws TException {
        return null;
    }

    public RequestVoteResp requestVote(long term, String candidateId, long lastLogIndex, long lastLogTerm) throws TException {
        return null;
    }
}
