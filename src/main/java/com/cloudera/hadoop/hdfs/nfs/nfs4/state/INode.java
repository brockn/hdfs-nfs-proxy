/**
 * Copyright 2012 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
  public long getCreationTime() {
    return creationTime;
  }

  public long getNumber() {
    return number;
  }

  public String getPath() {
    return path;
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
  public String toString() {
    return "INode [path=" + path + ", number=" + number + ", creationTime="
        + creationTime + "]";
  }
  
}
