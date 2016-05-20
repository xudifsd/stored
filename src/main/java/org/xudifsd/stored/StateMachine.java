package org.xudifsd.stored;

import java.nio.ByteBuffer;
import java.util.List;

public interface StateMachine {
    List<ByteBuffer> apply(List<ByteBuffer> entries);
}
