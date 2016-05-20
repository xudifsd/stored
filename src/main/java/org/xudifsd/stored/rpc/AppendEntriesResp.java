package org.xudifsd.stored.rpc;

public class AppendEntriesResp {
    public final long term;
    public final boolean success;

    public AppendEntriesResp(long term, boolean success) {
        this.term = term;
        this.success = success;
    }
}
