package org.xudifsd.stored.example;

import org.xudifsd.stored.StateMachine;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

public class MemoryKeyValueStateMachine implements StateMachine {
    private HashMap<String, String> map;

    public MemoryKeyValueStateMachine() {
        map = new HashMap<String, String>();
    }

    @Override
    public List<ByteBuffer> apply(List<ByteBuffer> entries) {
        // TODO do serialize & deserialize here
        return null;
    }
}