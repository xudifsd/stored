package org.xudifsd.stored;

public interface StateObserver {
    void stateChanged(RaftReactorState state);
}
