package com.linkedin.pinot.common.utils;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 12, 2014
 */

public class FileUtils {

  public static String getRandomFileName() {
    return StringUtil.join("-", "tmp", String.valueOf(System.currentTimeMillis()));
  }
}
