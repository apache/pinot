package com.linkedin.pinot.segments.v1.creator;


/**
 * Jun 30, 2014
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class V1Constants {
  public static class Numbers {
    // null representatives
    public static final Integer NULL_INT = Integer.MIN_VALUE;
    public static final Long NULL_LONG = Long.MIN_VALUE;
    public static final Float NULL_FLOAT = Float.MIN_VALUE;
    public static final Double NULL_DOUBLE = Double.MIN_VALUE;
  }
  
  public static class Str {
    public static final char STRING_PAD_CHAR = '%';
    public static final java.lang.String CHAR_SET = "UTF-8";
    public static final String NULL_STRING = "null";
  }
  
  public static class Dict {
    public static final int[] INT_DICTIONARY_COL_SIZE = new int[]{4};
    public static final int[] LONG_DICTIONARY_COL_SIZE = new int[]{8};
    public static final int[] FOLAT_DICTIONARY_COL_SIZE = new int[]{4};
    public static final int[] DOUBLE_DICTIONARY_COL_SIZE = new int[]{8};
  }
}
