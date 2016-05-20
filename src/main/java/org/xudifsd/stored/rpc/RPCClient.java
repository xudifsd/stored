package org.xudifsd.stored.rpc;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import org.xudifsd.stored.thrift.RaftProtocol;

import java.nio.ByteBuffer;
import java.util.List;

public class RPCClient {
    public final String ip;
    public final int port;

    // TODO add pool support

    public RPCClient(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public static AppendEntriesResp convert2LocalAppendEntriesResp(org.xudifsd.stored.thrift.AppendEntriesResp resp) {
        return new AppendEntriesResp(resp.term, resp.success);
    }

    public static RequestVoteResp convert2LocalRequestVoteResp(org.xudifsd.stored.thrift.RequestVoteResp resp) {
        return new RequestVoteResp(resp.term, resp.voteGranted);
    }

    public AppendEntriesResp appendEntries(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                                           List<ByteBuffer> entries, long leaderCommit) throws TException {
        TTransport transport = new TSocket(ip, port);
        TProtocol protocol = new TBinaryProtocol(transport);
        RaftProtocol.Client client = new RaftProtocol.Client(protocol);
        transport.open();
        return convert2LocalAppendEntriesResp(client.appendEntries(term, leaderId, prevLogIndex, prevLogTerm,
                entries, leaderCommit));
    }

    public RequestVoteResp requestVote(long term, String candidateId, long lastLogIndex, long lastLogTerm)
            throws TException {
        TTransport transport = new TSocket(ip, port);
        TProtocol protocol = new TBinaryProtocol(transport);
        RaftProtocol.Client client = new RaftProtocol.Client(protocol);
        transport.open();
        return convert2LocalRequestVoteResp(client.requestVote(term, candidateId, lastLogIndex, lastLogTerm));
    }
}
