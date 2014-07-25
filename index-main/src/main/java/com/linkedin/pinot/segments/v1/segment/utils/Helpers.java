package com.linkedin.pinot.segments.v1.segment.utils;

import org.apache.commons.lang.StringUtils;


public class Helpers {

  public static class STRING {
    public static String concat(char seperator, String... keys) {
      return StringUtils.join(keys, seperator);
    }

    public static String concat(String... keys) {
      return StringUtils.join(keys);
    }
  }

}
