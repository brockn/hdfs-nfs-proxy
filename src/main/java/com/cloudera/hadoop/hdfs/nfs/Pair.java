/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// stolen from MRUnit
package com.cloudera.hadoop.hdfs.nfs;

public class Pair<S, T> {

  private final S first;
  private final T second;

  public Pair(final S car, final T cdr) {
    first = car;
    second = cdr;
  }

  public S getFirst() { return first; }
  public T getSecond() { return second; }


  @Override
  public String toString() {
    String firstString = "null";
    String secondString = "null";

    if (null != first) {
      firstString = first.toString();
    }

    if (null != second) {
      secondString = second.toString();
    }

    return "(" + firstString + ", " + secondString + ")";
  }

  public static <S, T> Pair<S, T> of(S first , T second) {
    return new Pair<S, T>(first, second);
  }
}
