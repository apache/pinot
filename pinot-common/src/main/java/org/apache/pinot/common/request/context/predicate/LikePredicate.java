package org.apache.pinot.common.request.context.predicate;

import java.util.regex.Pattern;
import org.apache.pinot.common.request.context.ExpressionContext;

/**
 * Predicate which represents a LIKE operator
 */
public class LikePredicate extends RegexpLikePredicate {
  public static final String[] REGEXP_METACHARACTERS  = {"\\","^","$","{","}","[","]","(",")",".",
      "*","+","?","|","<",">","-","&"};

  public LikePredicate(ExpressionContext lhs, String value) {
    super(lhs, processValue(value));
  }

  @Override
  public Type getType() {
    return Type.LIKE;
  }

  @Override
  public String toString() {
    return _lhs + "LIKE" + _value;
  }

  private static String processValue(String value) {
    //String result = Pattern.quote(value);

    String result = escapeMetaCharacters(value);
    result = result.replace(".", "\\.");
    // ... escape any other potentially problematic characters here
    result = result.replace("?", ".");

    return result.replaceAll("(?<!\\\\)%", ".*");
  }

  /**
   * Add escape characters before special characters
   * @param inputString
   * @return
   */
  private static String escapeMetaCharacters(String inputString){

    for (int i = 0 ; i < REGEXP_METACHARACTERS.length ; i++){
      if(inputString.contains(REGEXP_METACHARACTERS[i])){
        inputString = inputString.replace(REGEXP_METACHARACTERS[i],"\\"
            + REGEXP_METACHARACTERS[i]);
      }
    }
    return inputString;
  }
}
