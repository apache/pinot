package com.linkedin.pinot.common;

import java.util.concurrent.atomic.AtomicLong;


public class Utils {

  private static final AtomicLong _uniqueIdGen = new AtomicLong(1);

  public static long getUniqueId() {
    return _uniqueIdGen.incrementAndGet();
  }

  /**
   * Takes a string, removes all characters that are not letters or digits and capitalizes the next letter following a
   * series of characters that are not letters or digits. For example, toCamelCase("Hello world!") returns "HelloWorld".
   *
   * @param text The text to camel case
   * @return The camel cased version of the string given
   */
  public static String toCamelCase(String text) {
    int length = text.length();
    StringBuilder builder = new StringBuilder(length);

    boolean capitalizeNextChar = false;

    for (int i = 0; i < length; i++) {
      char theChar = text.charAt(i);
      if (Character.isLetterOrDigit(theChar) || theChar == '.') {
        if (capitalizeNextChar) {
          builder.append(Character.toUpperCase(theChar));
          capitalizeNextChar = false;
        } else {
          builder.append(theChar);
        }
      } else {
        capitalizeNextChar = true;
      }
    }

    return builder.toString();
  }
}
