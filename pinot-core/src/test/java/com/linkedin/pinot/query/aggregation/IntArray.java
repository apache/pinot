package com.linkedin.pinot.query.aggregation;

/**
 * The datastructure that represents int array
 *
 */
public interface IntArray {
  public int size();

  public int get(int index);
}
