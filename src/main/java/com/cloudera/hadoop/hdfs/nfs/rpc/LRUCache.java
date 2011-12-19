package com.cloudera.hadoop.hdfs.nfs.rpc;


import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
  private static final long serialVersionUID = 1L;
  protected int mMaxSize;
  public LRUCache(int maxSize) {
    mMaxSize = maxSize;
  }
  @Override
  protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
    return size() > mMaxSize;
  }
}
