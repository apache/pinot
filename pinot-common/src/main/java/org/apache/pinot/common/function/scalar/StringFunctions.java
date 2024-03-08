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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Base64;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.RegexpPatternConverterUtils;
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
  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();


  /**
   * @see StringUtils#reverse(String)
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
    return StringUtils.substring(input, beginIndex);
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
    return StringUtils.substring(input, beginIndex, endIndex);
  }

  /**
   * @param input Parent string
   * @param beginIndex 1 based index from which substring should be created
   * @return substring from beginIndex to end of the parent string
   */
  @ScalarFunction
  public static String substring(String input, int beginIndex) {
    return StringUtils.substring(input, beginIndex - 1);
  }

  /**
   * Returns the substring of the main string from beginIndex of length.
   *
   * @param input Parent string
   * @param beginIndex 1 based index from which substring should be created
   * @param length length of substring to be created
   * @return a substirng of input string from beginIndex of length 'length'
   */
  @ScalarFunction
  public static String substring(String input, int beginIndex, int length) {
    // index is always 1 based
    beginIndex = beginIndex - 1;
    int endIndex = beginIndex + length;
    return StringUtils.substring(input, beginIndex, endIndex);
  }

  /**
   * Join two input string with seperator in between
   * @param input1
   * @param input2
   * @param seperator
   * @return The two input strings joined by the seperator
   */
  @ScalarFunction(names = "concat_ws")
  public static String concatws(String seperator, String input1, String input2) {
    return concat(input1, input2, seperator);
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
   * Join two input string with no seperator in between
   * @param input1
   * @param input2
   * @return The two input strings joined
   */
  @ScalarFunction
  public static String concat(String input1, String input2) {
    return concat(input1, input2, "");
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
   * Standard SQL trim function.
   *
   * @param end BOTH|LEADING|TRAILING
   * @param characters characters to be trimmed off
   * @param value value to trim
   * @return trim the characters from both/leading/trailing end of the string
   */
  @ScalarFunction
  public static String trim(String end, String characters, String value) {
    int length = value.length();
    int startIndex = 0;
    int endIndex = length;
    if (end.equals("BOTH") || end.equals("LEADING")) {
      while (startIndex < endIndex) {
        if (characters.indexOf(value.charAt(startIndex)) >= 0) {
          startIndex++;
        } else {
          break;
        }
      }
    }
    if (end.equals("BOTH") || end.equals("TRAILING")) {
      while (startIndex < endIndex) {
        if (characters.indexOf(value.charAt(endIndex - 1)) >= 0) {
          endIndex--;
        } else {
          break;
        }
      }
    }
    if (startIndex > 0 || endIndex < length) {
      return value.substring(startIndex, endIndex);
    } else {
      return value;
    }
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
   * @see StringUtils#left(String, int)
   * @param input
   * @return get substring starting from the first index and extending upto specified length.
   */
  @ScalarFunction
  public static String leftSubStr(String input, int length) {
    return StringUtils.left(input, length);
  }

  /**
   * @see StringUtils#right(String, int)
   * @param input
   * @return get substring ending at the last index with specified length
   */
  @ScalarFunction
  public static String rightSubStr(String input, int length) {
    return StringUtils.right(input, length);
  }

  /**
   * @see #StringFunctions#regexpExtract(String, String, int, String)
   * @param value
   * @param regexp
   * @return the matched result.
   */
  @ScalarFunction(names = {"regexp_extract", "regexpExtract"})
  public static String regexpExtract(String value, String regexp) {
    return regexpExtract(value, regexp, 0, "");
  }

  /**
   * @see #StringFunctions#regexpExtract(String, String, int, String)
   * @param value
   * @param regexp
   * @param group
   * @return the matched result.
   */
  @ScalarFunction(names = {"regexp_extract", "regexpExtract"})
  public static String regexpExtract(String value, String regexp, int group) {
    return regexpExtract(value, regexp, group, "");
  }

  /**
   * Regular expression that extract first matched substring.
   * @param value input value
   * @param regexp regular expression
   * @param group the group number within the regular expression to extract.
   * @param defaultValue the default value if no match found
   * @return the matched result
   */
  @ScalarFunction(names = {"regexp_extract", "regexpExtract"})
  public static String regexpExtract(String value, String regexp, int group, String defaultValue) {
    Pattern p = Pattern.compile(regexp);
    Matcher matcher = p.matcher(value);
    if (matcher.find() && matcher.groupCount() >= group) {
      return matcher.group(group);
    } else {
      return defaultValue;
    }
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
   * @see StringUtils#startsWith(CharSequence, CharSequence)
   * @param input
   * @param prefix substring to check if it is the prefix
   * @return true if string starts with prefix, false o.w.
   */
  @ScalarFunction(names = {"startsWith", "starts_with"})
  public static boolean startsWith(String input, String prefix) {
    return StringUtils.startsWith(input, prefix);
  }

  /**
   * @see StringUtils#endsWith(CharSequence, CharSequence)
   * @param input
   * @param suffix substring to check if it is the prefix
   * @return true if string ends with prefix, false o.w.
   */
  @ScalarFunction(names = {"endsWith", "ends_with"})
  public static boolean endsWith(String input, String suffix) {
    return StringUtils.endsWith(input, suffix);
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
   * @param bytes
   * @param charsetName encoding
   * @return bytearray to string
   * returns null on exception
   */
  @ScalarFunction
  public static String fromBytes(byte[] bytes, String charsetName) {
    try {
      return new String(bytes, charsetName);
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }

  /**
   * @param input
   * @param charsetName encoding
   * @return bytearray to string
   * returns null on exception
   */
  @ScalarFunction
  public static byte[] toBytes(String input, String charsetName) {
    try {
      return input.getBytes(charsetName);
    } catch (UnsupportedEncodingException e) {
      return null;
    }
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
   * @param input bytes
   * @return UTF8 encoded string
   */
  @ScalarFunction
  public static String fromUtf8(byte[] input) {
    return new String(input, StandardCharsets.UTF_8);
  }

  /**
   * @see StandardCharsets#US_ASCII#encode(String)
   * @param input
   * @return bytes
   */
  @ScalarFunction
  public static byte[] toAscii(String input) {
    return input.getBytes(StandardCharsets.US_ASCII);
  }

  /**
   * @param input bytes
   * @return ASCII encoded string
   */
  @ScalarFunction
  public static String fromAscii(byte[] input) {
    return new String(input, StandardCharsets.US_ASCII);
  }

  /**
   * @param input UUID as string
   * @return bytearray
   * returns bytes and null on exception
   */
  @ScalarFunction
  public static byte[] toUUIDBytes(String input) {
    try {
      UUID uuid = UUID.fromString(input);
      ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      return bb.array();
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * @param input UUID serialized to bytes
   * @return String representation of UUID
   * returns bytes and null on exception
   */
  @ScalarFunction
  public static String fromUUIDBytes(byte[] input) {
    ByteBuffer bb = ByteBuffer.wrap(input);
    long firstLong = bb.getLong();
    long secondLong = bb.getLong();
    return new UUID(firstLong, secondLong).toString();
  }

  /**
   * @see Normalizer#normalize(CharSequence, Normalizer.Form)
   * @param input
   * @return transforms string with NFC normalization form.
   */
  @ScalarFunction
  public static String normalize(String input) {
    return Normalizer.normalize(input, Normalizer.Form.NFC);
  }

  /**
   * @see Normalizer#normalize(CharSequence, Normalizer.Form)
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
   * @see StringUtils#split(String, String)
   * @param input
   * @param delimiter
   * @return splits string on specified delimiter and returns an array.
   */
  @ScalarFunction
  public static String[] split(String input, String delimiter) {
    return StringUtils.splitByWholeSeparator(input, delimiter);
  }

  /**
   * @param input
   * @param delimiter
   * @param limit
   * @return splits string on specified delimiter limiting the number of results till the specified limit
   */
  @ScalarFunction
  public static String[] split(String input, String delimiter, int limit) {
    return StringUtils.splitByWholeSeparator(input, delimiter, limit);
  }

  /**
   * TODO: Revisit if index should be one-based (both Presto and Postgres use one-based index, which starts with 1)
   * @param input
   * @param delimiter
   * @param index
   * @return splits string on specified delimiter and returns String at specified index from the split.
   */
  @ScalarFunction
  public static String splitPart(String input, String delimiter, int index) {
    String[] splitString = StringUtils.splitByWholeSeparator(input, delimiter);
    if (index < splitString.length) {
      return splitString[index];
    } else {
      return "null";
    }
  }

  /**
   * @see StringUtils#repeat(char, int)
   * @param input
   * @param times
   * @return concatenate the string to itself specified number of times
   */
  @ScalarFunction
  public static String repeat(String input, int times) {
    return StringUtils.repeat(input, times);
  }

  /**
   * @see StringUtils#repeat(String, String, int)
   * @param input
   * @param times
   * @return concatenate the string to itself specified number of times with specified seperator
   */
  @ScalarFunction
  public static String repeat(String input, String sep, int times) {
    return StringUtils.repeat(input, sep, times);
  }

  /**
   * @see StringUtils#remove(String, String)
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
   * @see String#contains(CharSequence)
   * @param input
   * @param substring
   * @return returns true if substring present in main string else false.
   */
  @ScalarFunction
  public static boolean contains(String input, String substring) {
    return input.contains(substring);
  }

  /**
   * Compare input strings lexicographically.
   * @return the value 0 if the first string argument is equal to second string; a value less than 0 if first string
   * argument is lexicographically less than the second string argument; and a value greater than 0 if the first string
   * argument is lexicographically greater than the second string argument.
   */
  @ScalarFunction
  public static int strcmp(String input1, String input2) {
    return input1.compareTo(input2);
  }

  /**
   *
   * @param input plaintext string
   * @return url encoded string
   * @throws UnsupportedEncodingException
   */
  @ScalarFunction
  public static String encodeUrl(String input)
      throws UnsupportedEncodingException {
    return URLEncoder.encode(input, StandardCharsets.UTF_8.toString());
  }

  /**
   *
   * @param input url encoded string
   * @return plaintext string
   * @throws UnsupportedEncodingException
   */
  @ScalarFunction
  public static String decodeUrl(String input)
      throws UnsupportedEncodingException {
    return URLDecoder.decode(input, StandardCharsets.UTF_8.toString());
  }

  /**
   * @param input binary data
   * @return Base64 encoded String
   */
  @ScalarFunction
  public static String toBase64(byte[] input) {
    return Base64.getEncoder().encodeToString(input);
  }

  /**
   * @param input Base64 encoded String
   * @return decoded binary data
   */
  @ScalarFunction
  public static byte[] fromBase64(String input) {
    return Base64.getDecoder().decode(input);
  }

  /**
   * Replace a regular expression pattern. If matchStr is not found, inputStr will be returned. By default, all
   * occurences of match pattern in the input string will be replaced. Default matching pattern is case sensitive.
   *
   * @param inputStr Input string to apply the regexpReplace
   * @param matchStr Regexp or string to match against inputStr
   * @param replaceStr Regexp or string to replace if matchStr is found
   * @param matchStartPos Index of inputStr from where matching should start. Default is 0.
   * @param occurence Controls which occurence of the matched pattern must be replaced. Counting starts at 0. Default
   *                  is -1
   * @param flag Single character flag that controls how the regex finds matches in inputStr. If an incorrect flag is
   *            specified, the function applies default case sensitive match. Only one flag can be specified. Supported
   *             flags:
   *             i -> Case insensitive
   * @return replaced input string
   */
  @ScalarFunction(names = {"regexpReplace", "regexp_replace"})
  public static String regexpReplace(String inputStr, String matchStr, String replaceStr, int matchStartPos,
      int occurence, String flag) {
    Integer patternFlag;

    // TODO: Support more flags like MULTILINE, COMMENTS, etc.
    switch (flag) {
      case "i":
        patternFlag = Pattern.CASE_INSENSITIVE;
        break;
      default:
        patternFlag = null;
        break;
    }

    Pattern p;
    if (patternFlag != null) {
      p = Pattern.compile(matchStr, patternFlag);
    } else {
      p = Pattern.compile(matchStr);
    }

    Matcher matcher = p.matcher(inputStr).region(matchStartPos, inputStr.length());
    StringBuffer sb;

    if (occurence >= 0) {
      sb = new StringBuffer(inputStr);
      while (occurence >= 0 && matcher.find()) {
        if (occurence == 0) {
          sb.replace(matcher.start(), matcher.end(), replaceStr);
          break;
        }
        occurence--;
      }
    } else {
      sb = new StringBuffer();
      while (matcher.find()) {
        matcher.appendReplacement(sb, replaceStr);
      }
      matcher.appendTail(sb);
    }

    return sb.toString();
  }

  /**
   * See #regexpReplace(String, String, String, int, int, String). Matches against entire inputStr and replaces all
   * occurences. Match is performed in case-sensitive mode.
   *
   * @param inputStr Input string to apply the regexpReplace
   * @param matchStr Regexp or string to match against inputStr
   * @param replaceStr Regexp or string to replace if matchStr is found
   * @return replaced input string
   */
  @ScalarFunction(names = {"regexpReplace", "regexp_replace"})
  public static String regexpReplace(String inputStr, String matchStr, String replaceStr) {
    return regexpReplace(inputStr, matchStr, replaceStr, 0, -1, "");
  }

  /**
   * See #regexpReplace(String, String, String, int, int, String). Matches against entire inputStr and replaces all
   * occurences. Match is performed in case-sensitive mode.
   *
   * @param inputStr Input string to apply the regexpReplace
   * @param matchStr Regexp or string to match against inputStr
   * @param replaceStr Regexp or string to replace if matchStr is found
   * @param matchStartPos Index of inputStr from where matching should start. Default is 0.
   * @return replaced input string
   */
  @ScalarFunction(names = {"regexpReplace", "regexp_replace"})
  public static String regexpReplace(String inputStr, String matchStr, String replaceStr, int matchStartPos) {
    return regexpReplace(inputStr, matchStr, replaceStr, matchStartPos, -1, "");
  }

  /**
   * See #regexpReplace(String, String, String, int, int, String). Match is performed in case-sensitive mode.
   *
   * @param inputStr Input string to apply the regexpReplace
   * @param matchStr Regexp or string to match against inputStr
   * @param replaceStr Regexp or string to replace if matchStr is found
   * @param matchStartPos Index of inputStr from where matching should start. Default is 0.
   * @param occurence Controls which occurence of the matched pattern must be replaced. Counting starts
   *                    at 0. Default is -1
   * @return replaced input string
   */
  @ScalarFunction(names = {"regexpReplace", "regexp_replace"})
  public static String regexpReplace(String inputStr, String matchStr, String replaceStr, int matchStartPos,
      int occurence) {
    return regexpReplace(inputStr, matchStr, replaceStr, matchStartPos, occurence, "");
  }

  @ScalarFunction(names = {"regexpLike", "regexp_like"})
  public static boolean regexpLike(String inputStr, String regexPatternStr) {
    Pattern pattern = Pattern.compile(regexPatternStr, Pattern.UNICODE_CASE | Pattern.CASE_INSENSITIVE);
    return pattern.matcher(inputStr).find();
  }

  @ScalarFunction
  public static boolean like(String inputStr, String likePatternStr) {
    String regexPatternStr = RegexpPatternConverterUtils.likeToRegexpLike(likePatternStr);
    return regexpLike(inputStr, regexPatternStr);
  }

  /**
   * Checks whether the input string can be parsed into a json node or not. Useful for scenarios where we want
   * to filter out malformed json. In case of nulls, it is treated as valid json as in partial-upsert we might
   * want to treat that column value as valid.
   *
   * @param inputStr Input string to test for valid json
   */
  @ScalarFunction
  public static boolean isJson(String inputStr) {
    try {
      if (inputStr == null) {
        return true;
      }
      OBJECT_MAPPER.readTree(inputStr);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
