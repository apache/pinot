package com.linkedin.pinot.segments.v1.segment.utils;


/**
 * Jul 15, 2014
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public interface IntArray {
  public abstract void setInt(final int index, final int value);
  public abstract int getInt(final int index);
  public abstract int size();
}
