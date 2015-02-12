package com.linkedin.pinot.core.indexsegment;

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
  COLUMNAR,
  SIMPLE;

  public static IndexType valueOfStr(String value) {
    return IndexType.valueOf(value.toUpperCase());
  }
}
