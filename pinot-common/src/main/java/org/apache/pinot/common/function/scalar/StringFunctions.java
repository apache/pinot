/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.function.scalar;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Inbuilt String Transformation Functions
 * The functions can be used as UDFs in Query when added in the FunctionRegistry.
 * @ScalarFunction annotation is used with each method for the registration
 *
 * Example usage:
 * <code> SELECT UPPER(playerName) FROM baseballStats LIMIT 10 </code>
 */
public class StringFunctions {
  private StringFunctions() {
  }

  private final static Pattern LTRIM = Pattern.compile("^\\s+");
  private final static Pattern RTRIM = Pattern.compile("\\s+$");

  /**
   * @see StringBuilder#reverse()
   * @param input
   * @return reversed input in from end to start
   */
  @ScalarFunction
  public static String reverse(String input) {
    return StringUtils.reverse(input);
  }

  /**
   * @see String#toLowerCase())
   * @param input
   * @return string in lower case format
   */
  @ScalarFunction
  public static String lower(String input) {
    return input.toLowerCase();
  }

  /**
   * @see String#toUpperCase()
   * @param input
   * @return string in upper case format
   */
  @ScalarFunction
  public static String upper(String input) {
    return input.toUpperCase();
  }

  /**
   * @see String#substring(int)
   * @param input Parent string
   * @param beginIndex index from which substring should be created
   * @return substring from beginIndex to end of the parent string
   */
  @ScalarFunction
  public static String substr(String input, int beginIndex) {
    return input.substring(beginIndex);
  }

  /**
   * Returns the substring of the main string from beginIndex to endIndex.
   * If endIndex is -1 returns the substring from begingIndex to end of the string.
   *
   * @see String#substring(int, int)
   * @param input Parent string
   * @param beginIndex index from which substring should be created
   * @param endIndex index at which substring should be terminated
   * @return substring from beginIndex to endIndex
   */
  @ScalarFunction
  public static String substr(String input, int beginIndex, int endIndex) {
    if (endIndex == -1) {
      return substr(input, beginIndex);
    }
    return input.substring(beginIndex, endIndex);
  }

  /**
   * Join two input string with seperator in between
   * @param input1
   * @param input2
   * @param seperator
   * @return The two input strings joined by the seperator
   */
  @ScalarFunction
  public static String concat(String input1, String input2, String seperator) {
    String result = input1;
    result = result + seperator + input2;
    return result;
  }

  /**
   * @see String#trim()
   * @param input
   * @return trim spaces from both ends of the string
   */
  @ScalarFunction
  public static String trim(String input) {
    return input.trim();
  }

  /**
   * @param input
   * @return trim spaces from left side of the string
   */
  @ScalarFunction
  public static String ltrim(String input) {
    return LTRIM.matcher(input).replaceAll("");
  }

  /**
   * @param input
   * @return trim spaces from right side of the string
   */
  @ScalarFunction
  public static String rtrim(String input) {
    return RTRIM.matcher(input).replaceAll("");
  }

  /**
   * @see String#length()
   * @param input
   * @return length of the string
   */
  @ScalarFunction
  public static int length(String input) {
    return input.length();
  }

  /**
   * @see StringUtils#ordinalIndexOf(CharSequence, CharSequence, int)
   * Return the Nth occurence of a substring within the String
   * @param input
   * @param find substring to find
   * @param instance Integer denoting the instance no.
   * @return start index of the Nth instance of substring in main string
   */
  @ScalarFunction
  public static int strpos(String input, String find, int instance) {
    return StringUtils.ordinalIndexOf(input, find, instance);
  }

  /**
   * @see StringUtils#indexOf(CharSequence, CharSequence)
   * Return the 1st occurence of a substring within the String
   * @param input
   * @param find substring to find
   * @return start index of the 1st instance of substring in main string
   */
  @ScalarFunction
  public static int strpos(String input, String find) {
    return StringUtils.indexOf(input, find);
  }

  /**
   * @see StringUtils#lastIndexOf(CharSequence, CharSequence)
   * Return the last occurence of a substring within the String
   * @param input
   * @param find substring to find
   * @return start index of the last instance of substring in main string
   */
  @ScalarFunction
  public static int strrpos(String input, String find) {
    return StringUtils.lastIndexOf(input, find);
  }

  /**
   * @see StringUtils#lastIndexOf(CharSequence, CharSequence, int)
   * Return the Nth occurence of a substring in string starting from the end of the string.
   * @param input
   * @param find substring to find
   * @param instance Integer denoting the instance no.
   * @return start index of the Nth instance of substring in main string starting from the end of the string.
   */
  @ScalarFunction
  public static int strrpos(String input, String find, int instance) {
    return StringUtils.lastIndexOf(input, find, instance);
  }

  /**
   * @see String#startsWith(String)
   * @param input
   * @param prefix substring to check if it is the prefix
   * @return true if string starts with prefix, false o.w.
   */
  @ScalarFunction
  public static boolean startsWith(String input, String prefix) {
    return input.startsWith(prefix);
  }

  /**
   * @see String#replaceAll(String, String)
   * @param input
   * @param find target substring to replace
   * @param substitute new substring to be replaced with target
   */
  @ScalarFunction
  public static String replace(String input, String find, String substitute) {
    return StringUtils.replace(input, find, substitute);
  }

  /**
   * @see StringUtils#rightPad(String, int, char)
   * @param input
   * @param size final size of the string
   * @param pad pad string to be used
   * @return string padded from the right side with pad to reach final size
   */
  @ScalarFunction
  public static String rpad(String input, int size, String pad) {
    return StringUtils.rightPad(input, size, pad);
  }

  /**
   * @see StringUtils#leftPad(String, int, char)
   * @param input
   * @param size final size of the string
   * @param pad pad string to be used
   * @return string padded from the left side with pad to reach final size
   */
  @ScalarFunction
  public static String lpad(String input, int size, String pad) {
    return StringUtils.leftPad(input, size, pad);
  }

  /**
   * @see String#codePointAt(int)
   * @param input
   * @return the Unicode codepoint of the first character of the string
   */
  @ScalarFunction
  public static int codepoint(String input) {
    return input.codePointAt(0);
  }

  /**
   * @see Character#toChars(int)
   * @param codepoint
   * @return the character corresponding to the Unicode codepoint
   */
  @ScalarFunction
  public static String chr(int codepoint) {
    char[] result = Character.toChars(codepoint);
    return new String(result);
  }

  /**
   * @see StandardCharsets#UTF_8#encode(String)
   * @param input
   * @return bytes
   */
  @ScalarFunction
  public static byte[] toUtf8(String input) {
    return input.getBytes(StandardCharsets.UTF_8);
  }

  /**
   * see Normalizer#normalize(String, Form)
   * @param input
   * @return transforms string with NFC normalization form.
   */
  @ScalarFunction
  public static String normalize(String input) {
    return Normalizer.normalize(input, Normalizer.Form.NFC);
  }

  /**
   * see Normalizer#normalize(String, Form)
   * @param input
   * @param form
   * @return transforms string with the specified normalization form
   */
  @ScalarFunction
  public static String normalize(String input, String form) {
    Normalizer.Form targetForm = Normalizer.Form.valueOf(form);
    return Normalizer.normalize(input, targetForm);
  }

  /**
   * see String#split(String)
   * @param input
   * @param delimiter
   * @return splits string on specified delimiter and returns an array.
   */
  @ScalarFunction
  public static String[] split(String input, String delimiter) {
    return StringUtils.split(input, delimiter);
  }

  /**
   * see String#replaceAll(String, String)
   * @param input
   * @param search
   * @return removes all instances of search from string
   */
  @ScalarFunction
  public static String remove(String input, String search) {
    return StringUtils.remove(input, search);
  }

  /**
   * @param input1
   * @param input2
   * @return returns the Hamming distance of input1 and input2, note that the two strings must have the same length.
   */
  @ScalarFunction
  public static int hammingDistance(String input1, String input2) {
    if (input1.length() != input2.length()) {
      return -1;
    }
    int distance = 0;
    for (int i = 0; i < input1.length(); i++) {
      if (input1.charAt(i) != input2.charAt(i)) {
        distance++;
      }
    }
    return distance;
  }

  /**
   * see String#contains(String)
   * @param input
   * @param substring
   * @return returns true if substring present in main string else false.
   */
  @ScalarFunction
  public static Boolean contains(String input, String substring) {
    return input.contains(substring);
  }
}
