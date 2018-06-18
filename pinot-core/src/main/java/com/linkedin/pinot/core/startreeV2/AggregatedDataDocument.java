package com.linkedin.pinot.core.startreeV2;

public class AggregatedDataDocument {

  private int _max = -1;
  private int _min = -1;
  private int _sum = -1;
  private int _count = -1;

  public void setMax(int max) {
     _max = max;
  }

  public void setMin(int min) {
    _min = min;
  }

  public void setSum(int sum) {
    _sum = sum;
  }

  public void setCount(int count) {
    _count = count;
  }
}
