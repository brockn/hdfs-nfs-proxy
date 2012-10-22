package com.cloudera.hadoop.hdfs.nfs.nfs4.state;

import java.io.Serializable;

public class INode implements Serializable {
  private static final long serialVersionUID = -348227331042789238L;
  private final String path;
  private final long number;
  private final long creationTime;
  public INode(String path, long number) {
    super();
    this.path = path;
    this.number = number;
    creationTime = System.currentTimeMillis();
  }
  public long getCreationTime() {
    return creationTime;
  }
  public String getPath() {
    return path;
  }

  public long getNumber() {
    return number;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (number ^ (number >>> 32));
    result = prime * result + ((path == null) ? 0 : path.hashCode());
    return result;
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    INode other = (INode) obj;
    if (number != other.number)
      return false;
    if (path == null) {
      if (other.path != null)
        return false;
    } else if (!path.equals(other.path))
      return false;
    return true;
  }
  
}
