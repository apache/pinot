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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Base64;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.JsonUtils;


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
   * Joins two input strings with no separator in between.
   */
  @ScalarFunction
  public static String concat(String input1, String input2) {
    return input1 + input2;
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
   * @see StringUtils#left(String, int)
   * @param input
   * @return get substring starting from the first index and extending upto specified length.
   */
  @ScalarFunction(names = {"leftSubStr", "left"})
  public static String leftSubStr(String input, int length) {
    return StringUtils.left(input, length);
  }

  /**
   * @see StringUtils#right(String, int)
   * @param input
   * @return get substring ending at the last index with specified length
   */
  @ScalarFunction(names = {"rightSubStr", "right"})
  public static String rightSubStr(String input, int length) {
    return StringUtils.right(input, length);
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
  @ScalarFunction
  public static boolean startsWith(String input, String prefix) {
    return StringUtils.startsWith(input, prefix);
  }

  /**
   * @see StringUtils#endsWith(CharSequence, CharSequence)
   * @param input
   * @param suffix substring to check if it is the prefix
   * @return true if string ends with prefix, false o.w.
   */
  @ScalarFunction
  public static boolean endsWith(String input, String suffix) {
    return StringUtils.endsWith(input, suffix);
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
  @ScalarFunction(names = {"split", "stringToArray"})
  public static String[] split(String input, String delimiter) {
    return StringUtils.splitByWholeSeparator(input, delimiter);
  }

  /**
   * @param input
   * @param delimiter
   * @param limit
   * @return splits string on specified delimiter limiting the number of results till the specified limit
   */
  @ScalarFunction(names = {"split", "stringToArray"})
  public static String[] split(String input, String delimiter, int limit) {
    return StringUtils.splitByWholeSeparator(input, delimiter, limit);
  }

  /**
   * @param input an input string for prefix strings generations.
   * @param maxlength the max length of the prefix strings for the string.
   * @return generate an array of prefix strings of the string that are shorter than the specified length.
   */
  @ScalarFunction
  public static String[] prefixes(String input, int maxlength) {
    int arrLength = Math.min(maxlength, input.length());
    String[] prefixArr = new String[arrLength];
    for (int prefixIdx = 1; prefixIdx <= arrLength; prefixIdx++) {
      prefixArr[prefixIdx - 1] = input.substring(0, prefixIdx);
    }
    return prefixArr;
  }

  /**
   * @param input an input string for prefix strings generations.
   * @param maxlength the max length of the prefix strings for the string.
   * @param prefix the prefix to be prepended to prefix strings generated. e.g. '^' for regex matching
   * @return generate an array of prefix matchers of the string that are shorter than the specified length.
   */
  @ScalarFunction(nullableParameters = true)
  public static String[] prefixesWithPrefix(String input, int maxlength, @Nullable String prefix) {
    if (prefix == null) {
      return prefixes(input, maxlength);
    }
    int arrLength = Math.min(maxlength, input.length());
    String[] prefixArr = new String[arrLength];
    for (int prefixIdx = 1; prefixIdx <= arrLength; prefixIdx++) {
      prefixArr[prefixIdx - 1] = prefix + input.substring(0, prefixIdx);
    }
    return prefixArr;
  }

  /**
   * @param input an input string for suffix strings generations.
   * @param maxlength the max length of the suffix strings for the string.
   * @return generate an array of suffix strings of the string that are shorter than the specified length.
   */
  @ScalarFunction
  public static String[] suffixes(String input, int maxlength) {
    int arrLength = Math.min(maxlength, input.length());
    String[] suffixArr = new String[arrLength];
    for (int suffixIdx = 1; suffixIdx <= arrLength; suffixIdx++) {
      suffixArr[suffixIdx - 1] = input.substring(input.length() - suffixIdx);
    }
    return suffixArr;
  }

  /**
   * @param input an input string for suffix strings generations.
   * @param maxlength the max length of the suffix strings for the string.
   * @param suffix the suffix string to be appended for suffix strings generated. e.g. '$' for regex matching.
   * @return generate an array of suffix matchers of the string that are shorter than the specified length.
   */
  @ScalarFunction(nullableParameters = true)
  public static String[] suffixesWithSuffix(String input, int maxlength, @Nullable String suffix) {
    if (suffix == null) {
      return suffixes(input, maxlength);
    }
    int arrLength = Math.min(maxlength, input.length());
    String[] suffixArr = new String[arrLength];
    for (int suffixIdx = 1; suffixIdx <= arrLength; suffixIdx++) {
      suffixArr[suffixIdx - 1] = input.substring(input.length() - suffixIdx) + suffix;
    }
    return suffixArr;
  }

  /**
   * TODO: Revisit if index should be one-based (both Presto and Postgres use one-based index, which starts with 1)
   * @param input
   * @param delimiter
   * @param index we allow negative value for index which indicates the index from the end.
   * @return splits string on specified delimiter and returns String at specified index from the split.
   */
  @ScalarFunction
  public static String splitPart(String input, String delimiter, int index) {
    String[] splitString = StringUtils.splitByWholeSeparator(input, delimiter);
    if (index >= 0 && index < splitString.length) {
      return splitString[index];
    } else if (index < 0 && index >= -splitString.length) {
      return splitString[splitString.length + index];
    } else {
      return "null";
    }
  }

  /**
   * @param input the input String to be split into parts.
   * @param delimiter the specified delimiter to split the input string.
   * @param limit the max count of parts that the input string can be splitted into.
   * @param index the specified index for the splitted parts to be returned.
   * @return splits string on the delimiter with the limit count and returns String at specified index from the split.
   */
  @ScalarFunction
  public static String splitPart(String input, String delimiter, int limit, int index) {
    String[] splitString = StringUtils.splitByWholeSeparator(input, delimiter, limit);
    if (index >= 0 && index < splitString.length) {
      return splitString[index];
    } else if (index < 0 && index >= -splitString.length) {
      return splitString[splitString.length + index];
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
   */
  @ScalarFunction
  public static String encodeUrl(String input) {
    return URIUtils.encode(input);
  }

  /**
   *
   * @param input url encoded string
   * @return plaintext string
   */
  @ScalarFunction
  public static String decodeUrl(String input) {
    return URIUtils.decode(input);
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
   * Checks whether the input string can be parsed into a json node or not. Useful for scenarios where we want
   * to filter out malformed json.
   * Null values are handled by the function invoker here and this function processes the results on non-null values.
   *
   * @param inputStr Input string to test for valid json
   * @return  true in case of valid json parsing else false
   *
   */
  @ScalarFunction
  public static boolean isJson(String inputStr) {
    try {
      JsonUtils.stringToJsonNode(inputStr);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
