package org.xudifsd.stored.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RPCHandlerProcessor extends org.xudifsd.stored.thrift.RaftProtocol.Processor<RPCHandler> {
    private static final Logger LOG = LoggerFactory.getLogger(RPCHandlerProcessor.class);

    public RPCHandlerProcessor(RPCHandler iface) {
        super(iface);
    }

    public static class appendEntries
        extends org.xudifsd.stored.thrift.RaftProtocol.Processor.appendEntries<RPCHandler> {
        public org.xudifsd.stored.thrift.RaftProtocol.appendEntries_result getResult(
                RPCHandler iface,
                org.xudifsd.stored.thrift.RaftProtocol.appendEntries_args args)
                throws org.apache.thrift.TException {
            // TODO add metrics stats
            return super.getResult(iface, args);
        }
    }

    public static class requestVote
            extends org.xudifsd.stored.thrift.RaftProtocol.Processor.requestVote<RPCHandler> {
        public org.xudifsd.stored.thrift.RaftProtocol.requestVote_result getResult(
                RPCHandler iface,
                org.xudifsd.stored.thrift.RaftProtocol.requestVote_args args)
                throws org.apache.thrift.TException {
            // TODO add metrics stats
            return super.getResult(iface, args);
        }
    }
}
