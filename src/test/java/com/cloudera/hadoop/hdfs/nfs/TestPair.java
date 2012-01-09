package com.cloudera.hadoop.hdfs.nfs;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestPair {

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
    assertEquals(left, pair.getFirst());    
    assertEquals(right, pair.getSecond());
  }
}
