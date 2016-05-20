package org.xudifsd.stored;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class RaftReactorTest extends TestCase {
    public RaftReactorTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(RaftReactorTest.class);
    }

    public void testApp() {
        assertTrue(true);
    }
}
