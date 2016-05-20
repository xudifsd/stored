package org.xudifsd.stored;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateLogger implements StateObserver {
    private static final Logger LOG = LoggerFactory.getLogger(StateLogger.class);
    private RaftReactorState state;

    public StateLogger(RaftReactorState state) {
        this.state = state;
        LOG.info("init state is {}", this.state);
    }

    public void stateChanged(RaftReactorState state) {
        LOG.info("state change from {} -> {}", this.state, state);
        this.state = state;
    }
}
