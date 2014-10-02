package com.linkedin.pinot.common.utils;

import org.apache.commons.lang.StringUtils;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 30, 2014
 */

public class StringUtil {

  public static String join(String seperator, String...keys) {
    return StringUtils.join(keys, seperator);
  }
}
