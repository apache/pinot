package com.linkedin.pinot.core.query.aggregation.groupby;

public class BitHacks {
  private static int[] LogTable256 = new int[256];
  static {
    LogTable256[1] = 0;
    for (int i = 2; i < 256; i++) {
      LogTable256[i] = 1 + LogTable256[i / 2];
    }
    LogTable256[0] = -1;
  }

  public static int findLogBase2(int v) {
    int r;
    int tt;
    if ((tt = v >>> 24) > 0) {
      r = 24 + LogTable256[tt];
    } else if ((tt = v >>> 16) > 0) {
      r = 16 + LogTable256[tt];
    } else if ((tt = v >>> 8) > 0) {
      r = 8 + LogTable256[tt];
    } else {
      r = LogTable256[v];
    }
    return r;
  }

  public static void main(String[] args) {
    System.out.println(~0L);
    int v = 4;
    int pos = findLogBase2(v);
    System.out.println("leftmost on bit for " + v + " is: " + pos);
    int x = -3;
    v = ~x;
    pos = findLogBase2(v);
    System.out.println("leftmost off bit for " + x + " is: " + pos);
    System.out.println("leftmost on bit for 0 is: " + findLogBase2(0));
    System.out.println("leftmost off bit for -1 is: " + findLogBase2(~(-1)));
  }
}
