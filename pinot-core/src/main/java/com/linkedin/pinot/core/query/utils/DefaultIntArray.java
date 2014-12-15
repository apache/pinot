package com.linkedin.pinot.core.query.utils;

public class DefaultIntArray implements IntArray {
  private final int[] _arr;

  public DefaultIntArray(int[] arr) {
    this._arr = arr;
  }

  @Override
  public int size() {
    return _arr.length;
  }

  @Override
  public int get(int index) {
    return _arr[index];
  }

}
