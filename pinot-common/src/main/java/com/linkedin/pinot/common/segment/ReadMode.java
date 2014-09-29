package com.linkedin.pinot.common.segment;

public enum ReadMode {
  heap,
  mmap;

  public static ReadMode getEnum(String strVal) {
    if (strVal.equalsIgnoreCase("heap")) {
      return heap;
    }
    if (strVal.equalsIgnoreCase("mmap") || strVal.equalsIgnoreCase("memorymapped")
        || strVal.equalsIgnoreCase("memorymap")) {
      return mmap;
    }
    throw new IllegalArgumentException("Unknown String Value: " + strVal);
  }
}
