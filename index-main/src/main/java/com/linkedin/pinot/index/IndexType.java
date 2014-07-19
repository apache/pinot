package com.linkedin.pinot.index;

/**
 * IndexType shows supported indexes from very top level. Now only supports
 * columnar index, which is GAZELLE_INDEX.
 * In the future can support more index types, like row based or tree based
 * structure.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 * 
 */
public enum IndexType {
  columnar,
  simple;

  public static IndexType valueOfStr(String value) {
    for (IndexType indexType : IndexType.values()) {
      if (indexType.toString().toLowerCase().equals(value.toLowerCase())) {
        return indexType;
      }
    }
    throw new UnsupportedOperationException("Unsupported Index Type - " + value);
  }
}
