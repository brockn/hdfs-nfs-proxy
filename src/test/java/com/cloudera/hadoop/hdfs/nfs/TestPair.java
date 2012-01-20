package com.cloudera.hadoop.hdfs.nfs;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestPair {

    protected static final Logger LOGGER = Logger.getLogger(TestPair.class);

    @Before
    public void setup() {
        LOGGER.debug("setup");
    }

    @Test
    public void testToString() {
        Pair<Object, Object> pair = Pair.of(null, null);
        pair.toString(); // does not throw NPE
    }

    @Test
    public void testGetters() {
        Object left = new Object();
        Object right = new Object();
        Pair<Object, Object> pair = Pair.of(left, right);
        Assert.assertEquals(left, pair.getFirst());
        Assert.assertEquals(right, pair.getSecond());
    }
}
