package org.xudifsd.stored;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class StateMachineTest extends TestCase {
    public StateMachineTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(StateMachineTest.class);
    }

    public void testApp() {
        assertTrue(true);
    }
}
