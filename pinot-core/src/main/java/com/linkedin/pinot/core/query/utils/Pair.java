package com.linkedin.pinot.core.query.utils;

public class Pair<T1, T2> {

  private T1 _first;
  private T2 _second;

  public Pair(T1 first, T2 second) {
    _first = first;
    _second = second;
  }

  public T1 getFirst() {
    return _first;
  }

  public T2 getSecond() {
    return _second;
  }
}
