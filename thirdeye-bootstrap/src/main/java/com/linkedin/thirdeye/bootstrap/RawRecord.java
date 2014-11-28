package com.linkedin.thirdeye.bootstrap;

public class RawRecord implements Comparable<RawRecord>{

  String[] dimensions;
  
  int[] metrics;
  
  long time;
  
  @Override
  public int compareTo(RawRecord o) {
    return 0;
  }
  
}
