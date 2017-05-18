package com.linkedin.thirdeye.dashboard.views;

import java.util.HashSet;
import java.util.Set;

import com.linkedin.thirdeye.datasource.comparison.Row;

public class TimeBucket implements Comparable<TimeBucket> {

  long currentStart;
  long currentEnd;
  long baselineStart;
  long baselineEnd;

  public TimeBucket() {

  }

  public TimeBucket(long currentStart, long currentEnd, long baselineStart, long baselineEnd) {
    super();
    this.currentStart = currentStart;
    this.currentEnd = currentEnd;
    this.baselineStart = baselineStart;
    this.baselineEnd = baselineEnd;
  }

  public long getCurrentStart() {
    return currentStart;
  }

  public void setCurrentStart(long currentStart) {
    this.currentStart = currentStart;
  }

  public long getCurrentEnd() {
    return currentEnd;
  }

  public void setCurrentEnd(long currentEnd) {
    this.currentEnd = currentEnd;
  }

  public long getBaselineStart() {
    return baselineStart;
  }

  public void setBaselineStart(long baselineStart) {
    this.baselineStart = baselineStart;
  }

  public long getBaselineEnd() {
    return baselineEnd;
  }

  public void setBaselineEnd(long baselineEnd) {
    this.baselineEnd = baselineEnd;
  }

  @Override
  public int compareTo(TimeBucket that) {
    return Long.compare(this.getCurrentStart(), that.getCurrentStart());
  }

  @Override
  public String toString() {
    return String.format("%s-%s %s-%s", baselineStart, baselineEnd, currentStart, currentEnd);
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    TimeBucket that = (TimeBucket) obj;
    return this.baselineStart == that.baselineStart && this.baselineEnd == that.baselineEnd
        && this.currentStart == that.currentStart && this.currentEnd == that.currentEnd;
  }

  public static TimeBucket fromRow(Row row) {
    TimeBucket bucket = new TimeBucket();
    bucket.setBaselineStart(row.getBaselineStart().getMillis());
    bucket.setBaselineEnd(row.getBaselineEnd().getMillis());
    bucket.setCurrentStart(row.getCurrentStart().getMillis());
    bucket.setCurrentEnd(row.getCurrentEnd().getMillis());
    return bucket;
  }

  public static void main(String[] args) {
    Set<TimeBucket> set = new HashSet<>();
    set.add(new TimeBucket(1, 2, 3, 4));
    set.add(new TimeBucket(1, 2, 3, 4));
    System.out.println(set);
  }
}
