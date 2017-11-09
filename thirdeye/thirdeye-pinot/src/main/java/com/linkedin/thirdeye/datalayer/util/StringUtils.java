package com.linkedin.thirdeye.datalayer.util;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class StringUtils {
  private static Splitter SEMICOLON_SPLITTER = Splitter.on(";").omitEmptyStrings();
  private static Splitter EQUALS_SPLITTER = Splitter.on("=").omitEmptyStrings();

  private static Joiner SEMICOLON = Joiner.on(";");
  private static Joiner EQUALS = Joiner.on("=");

  /**
   * Encode the properties into String, which is in the format of field1=value1;field2=value2 and so on
   * @param props the properties instance
   * @return a String representation of the given properties
   */
  public static String encodeCompactedProperties(Properties props) {
    List<String> parts = new ArrayList<>();
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      parts.add(EQUALS.join(entry.getKey(), entry.getValue()));
    }
    return SEMICOLON.join(parts);
  }

  /**
   * Decode the properties string (e.g. field1=value1;field2=value2) to a properties instance
   * @param propStr a properties string in the format of field1=value1;field2=value2
   * @return a properties instance
   */
  public static Properties decodeCompactedProperties(String propStr) {
    Properties props = new Properties();
    for (String part : SEMICOLON_SPLITTER.split(propStr)) {
      List<String> kvPair = EQUALS_SPLITTER.splitToList(part);
      props.setProperty(kvPair.get(0), kvPair.get(1));
    }
    return props;
  }
}
