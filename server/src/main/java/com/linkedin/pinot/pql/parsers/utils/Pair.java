package com.linkedin.pinot.pql.parsers.utils;

import java.io.Serializable;


public class Pair<FIRST, SECOND> implements Serializable {
  private FIRST first;
  private SECOND second;

  public FIRST getFirst() {
    return first;
  }

  public void setFirst(FIRST first) {
    this.first = first;
  }

  public SECOND getSecond() {
    return second;
  }

  public void setSecond(SECOND second) {
    this.second = second;
  }

  public Pair(FIRST first, SECOND second) {
    super();
    this.first = first;
    this.second = second;
  }

}
