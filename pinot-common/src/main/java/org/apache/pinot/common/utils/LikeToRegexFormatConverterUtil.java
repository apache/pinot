package org.apache.pinot.common.utils;

/**
 * Utility for converting LIKE operator syntax to a regex
 */
public class LikeToRegexFormatConverterUtil {
  /* Represents all metacharacters to be processed */
  public static final String[] REGEXP_METACHARACTERS  = {"\\","^","$","{","}","[","]","(",")",
      "*","+","?","|","<",">","-","&"};

  /**
   * Process an incoming LIKE string and make it regexp friendly
   * @param value LIKE operator styled predicate
   * @return Result regex
   */
  public static String processValue(String value) {
    String result = escapeMetaCharacters(value);

    result = result.replace(".", "\\.");
    // ... escape any other potentially problematic characters here
    result = result.replace("?", ".");

    return result.replaceAll("(?<!\\\\)%", ".*");
  }

  /**
   * Add escape characters before special characters
   */
  private static String escapeMetaCharacters(String inputString) {

    for (int i = 0 ; i < REGEXP_METACHARACTERS.length ; i++){
      if(inputString.contains(REGEXP_METACHARACTERS[i])){
        inputString = inputString.replace(REGEXP_METACHARACTERS[i],"\\"
            + REGEXP_METACHARACTERS[i]);
      }
    }
    return inputString;
  }
}
