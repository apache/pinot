package com.linkedin.pinot.core.indexsegment.utils;

import org.apache.commons.lang.StringUtils;


/**
 * 
 * @author Dhaval Patel<dpatel@linkedin.com
 * Aug 19, 2014
 */
@Deprecated //why do we need another wrapper, we should use Gauva libraries
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
