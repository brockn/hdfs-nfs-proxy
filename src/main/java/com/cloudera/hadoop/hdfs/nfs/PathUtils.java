package com.cloudera.hadoop.hdfs.nfs;

import org.apache.hadoop.fs.Path;

public class PathUtils {

  public static String checkPath(String path) {
    if(path.contains(":")) {
      throw new IllegalArgumentException("Colon is disallowed in path names");
    }
    return path;
  }
  
  public static String realPath(Path path) {
    return path.toUri().getPath();
  }

}
