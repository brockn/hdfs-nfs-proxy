package com.cloudera.hadoop.hdfs.nfs;

// stolen from MRUnit
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
