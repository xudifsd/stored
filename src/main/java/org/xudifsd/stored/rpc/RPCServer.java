package org.xudifsd.stored.rpc;

import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RPCServer {
    private static final Logger LOG = LoggerFactory.getLogger(RPCServer.class);

    private TNonblockingServerSocket socket;
    private TServer server;
    private int listenPort;

    private ServeThread rpcThread;
    private RPCHandler handler;

    public RPCServer(RPCHandler handler, int listenPort) {
        this.listenPort = listenPort;
        this.handler = handler;
    }

    private class ServeThread extends Thread {
        private int workThreadNum;

        public ServeThread(int workThreadNum) {
            this.workThreadNum = workThreadNum;
        }

        @Override
        public void run() {
            TProcessor processor = new RPCHandlerProcessor(handler);

            TThreadedSelectorServer.Args arg = new TThreadedSelectorServer.Args(socket);
            arg.workerThreads(this.workThreadNum);
            arg.maxReadBufferBytes = 128 * 1024 * 1024;
            arg.protocolFactory(new TBinaryProtocol.Factory());
            arg.transportFactory(new TFramedTransport.Factory(32 * 1024 * 1024));
            arg.processorFactory(new TProcessorFactory(processor));
            server = new TThreadedSelectorServer(arg);
            server.serve();
        }
    }

    public void start(int workThreadNum) throws Exception {
        if (rpcThread != null && rpcThread.isAlive()) {
            LOG.warn("RPCServer: the rpc thread is running");
        } else {
            LOG.info("RPCServer: start on port " + listenPort);
            socket = new TNonblockingServerSocket(listenPort);
            rpcThread = new ServeThread(workThreadNum);
            rpcThread.start();
        }
    }

    public void stop() {
        if (rpcThread == null || !rpcThread.isAlive()) {
            LOG.warn("RPCServer: the rpc thread has stopped");
        } else {
            LOG.info("RPCServer: stop port " + listenPort);
            try {
                if (server != null) {
                    server.stop();
                }
                if (socket != null) {
                    socket.close();
                }
                rpcThread.join(1000);
            } catch (InterruptedException e) {
                LOG.error("RPCServer stop exception.", e);
            }
        }
    }
}
