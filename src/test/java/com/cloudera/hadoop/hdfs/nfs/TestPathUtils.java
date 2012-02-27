package com.cloudera.hadoop.hdfs.nfs;

import static org.junit.Assert.*;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestPathUtils {

  @Test(expected=IllegalArgumentException.class)
  public void testCheckPath() {
    PathUtils.checkPath(":");
  }

  @Test
  public void testRealPath() {
    assertEquals(PathUtils.realPath(new Path("file:///")), "/");
  }
}
