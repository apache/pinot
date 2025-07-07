package org.apache.pinot.query.planner.physical.v2;

public enum DistHashFunction {
  MURMUR,
  MURMUR3,
  HASHCODE,
  ABSHASHCODE;

  public static boolean isSupported(String hashFunction) {
    try {
      DistHashFunction.valueOf(hashFunction.toUpperCase());
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   *
   */
  public static DistHashFunction defaultFunction() {
    return MURMUR;
  }
}
