package org.xudifsd.stored.rpc;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xudifsd.stored.thrift.RaftProtocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class RPCClient {
    private static final Logger LOG = LoggerFactory.getLogger(RPCClient.class);

    public final InetSocketAddress address;

    // TODO add pool support

    public RPCClient(InetSocketAddress address) {
        this.address = address;
    }

    public static AppendEntriesResp convert2LocalAppendEntriesResp(org.xudifsd.stored.thrift.AppendEntriesResp resp) {
        return new AppendEntriesResp(resp.term, resp.success);
    }

    public static RequestVoteResp convert2LocalRequestVoteResp(org.xudifsd.stored.thrift.RequestVoteResp resp) {
        return new RequestVoteResp(resp.term, resp.voteGranted);
    }

    public AppendEntriesResp appendEntries(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                                           List<ByteBuffer> entries, long leaderCommit) throws TException {
        TTransport transport = new TSocket(address.getHostName(), address.getPort());
        TProtocol protocol = new TBinaryProtocol(transport);
        RaftProtocol.Client client = new RaftProtocol.Client(protocol);
        transport.open();
        return convert2LocalAppendEntriesResp(client.appendEntries(term, leaderId, prevLogIndex, prevLogTerm,
                entries, leaderCommit));
    }

    public RequestVoteResp requestVote(long term, String candidateId, long lastLogIndex, long lastLogTerm)
            throws TException {
        TTransport transport = new TSocket(address.getHostName(), address.getPort());
        TProtocol protocol = new TBinaryProtocol(transport);
        RaftProtocol.Client client = new RaftProtocol.Client(protocol);
        transport.open();
        return convert2LocalRequestVoteResp(client.requestVote(term, candidateId, lastLogIndex, lastLogTerm));
    }

    private class AppendEntriesCallback implements AsyncMethodCallback<RaftProtocol.AsyncClient.appendEntries_call> {
        private final BlockingQueue<AppendEntriesResp> queue;

        public AppendEntriesCallback(BlockingQueue<AppendEntriesResp> queue) {
            this.queue = queue;
        }

        @Override
        public void onComplete(RaftProtocol.AsyncClient.appendEntries_call appendEntries_call) {
            try {
                AppendEntriesResp resp = convert2LocalAppendEntriesResp(appendEntries_call.getResult());
                queue.offer(resp);
            } catch (TException e) {
                // TODO put something in queue, so that Async caller could return immediately
                LOG.error("getResult failed", e);
            }
        }

        @Override
        public void onError(Exception e) {
            // TODO put something in queue, so that Async caller could return immediately
            LOG.error("async call failed", e);
        }
    }

    public void asyncAppendEntries(long term, String leaderId, long prevLogIndex, long prevLogTerm,
           List<ByteBuffer> entries, long leaderCommit, BlockingQueue<AppendEntriesResp> queue)
           throws TException, IOException {
        TTransport transport = new TSocket(address.getHostName(), address.getPort());
        TProtocol protocol = new TBinaryProtocol(transport);
        RaftProtocol.AsyncClient asyncClient = new RaftProtocol.AsyncClient(new TBinaryProtocol.Factory(),
                new TAsyncClientManager(), new TNonblockingSocket(address.getHostName(), address.getPort()));

        AppendEntriesCallback callback = new AppendEntriesCallback(queue);
        asyncClient.appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit, callback);
    }

    private class RequestVoteCallback implements AsyncMethodCallback<RaftProtocol.AsyncClient.requestVote_call> {
        private final BlockingQueue<RequestVoteResp> queue;

        public RequestVoteCallback(BlockingQueue<RequestVoteResp> queue) {
            this.queue = queue;
        }

        @Override
        public void onComplete(RaftProtocol.AsyncClient.requestVote_call requestVote_call) {
            try {
                RequestVoteResp resp = convert2LocalRequestVoteResp(requestVote_call.getResult());
                queue.offer(resp);
            } catch (TException e) {
                // TODO put something in queue, so that Async caller could return immediately
                LOG.error("getResult failed", e);
            }
        }

        @Override
        public void onError(Exception e) {
            // TODO put something in queue, so that Async caller could return immediately
            LOG.error("async call failed", e);
        }
    }

    public void asyncRequestVote(long term, String candidateId, long lastLogIndex, long lastLogTerm,
           BlockingQueue<RequestVoteResp> queue) throws TException, IOException {
        TTransport transport = new TSocket(address.getHostName(), address.getPort());
        TProtocol protocol = new TBinaryProtocol(transport);
        RaftProtocol.AsyncClient asyncClient = new RaftProtocol.AsyncClient(new TBinaryProtocol.Factory(),
                new TAsyncClientManager(), new TNonblockingSocket(address.getHostName(), address.getPort()));

        RequestVoteCallback callback = new RequestVoteCallback(queue);
        asyncClient.requestVote(term, candidateId, lastLogIndex, lastLogTerm, callback);
    }
}
