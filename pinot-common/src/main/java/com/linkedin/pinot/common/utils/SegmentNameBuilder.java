package com.linkedin.pinot.common.utils;



/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 11, 2014
 */

public class SegmentNameBuilder {

  public static String buildBasic(String resourceName, String tableName, Object minTimeValue, Object maxTimeValue, String prefix) {
    return StringUtil.join("_", resourceName, tableName, minTimeValue.toString(), maxTimeValue.toString(), prefix);
  }
}
