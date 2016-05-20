package org.xudifsd.stored.rpc;

public class RequestVoteResp {
    public final long term;
    public final boolean voteGranted;

    public RequestVoteResp(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }
}
