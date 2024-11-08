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
package org.apache.pinot.core.util;

/**
 * Utility class with various number related methods.
 */
public class NumberUtils {

  public static final NumberFormatException NULL_EXCEPTION = new NumberFormatException("Can't parse null string");
  public static final NumberFormatException EXP_EXCEPTION = new NumberFormatException("Wrong exponent");
  public static final NumberFormatException EXCEPTION = new NumberFormatException("Can't parse value");

  private static final long[] POWERS_OF_10 = new long[]{
      1L,
      10L,
      100L,
      1000L,
      10000L,
      100000L,
      1000000L,
      10000000L,
      100000000L,
      1000000000L,
      10000000000L,
      100000000000L,
      1000000000000L,
      10000000000000L,
      100000000000000L,
      1000000000000000L,
      10000000000000000L,
      100000000000000000L,
      1000000000000000000L,
  };

  private NumberUtils() {
  }

  /**
   * Parses whole input char sequence.
   * Throws static, pre-allocated NumberFormatException.
   * If proper stack trace is required, caller has to catch it and throw another exception.
   * @param cs char sequence to parse
   * @return parsed long value
   */
  public static long parseLong(CharSequence cs) {
    if (cs == null) {
      throw NULL_EXCEPTION;
    }
    return parseLong(cs, 0, cs.length());
  }

  /**
   * Parses input char sequence between given indices.
   * Throws static, pre-allocated NumberFormatException.
   * If proper stack trace is required, caller has to catch it and throw another exception.
   * @param cs char sequence to parse
   * @param start start index (inclusive)
   * @param end end index (exclusive)
   * @return parsed long value
   */
  public static long parseLong(CharSequence cs, int start, int end) {
    if (cs == null) {
      throw NULL_EXCEPTION;
    }

    boolean negative = false;
    int i = start;
    long limit = -Long.MAX_VALUE;

    if (end > start) {
      char firstChar = cs.charAt(start);
      if (firstChar < '0') { // Possible leading "+" or "-"
        if (firstChar == '-') {
          negative = true;
          limit = Long.MIN_VALUE;
        } else if (firstChar != '+') {
          throw EXCEPTION;
        }

        if (end == start + 1) { // Cannot have lone "+" or "-"
          throw EXCEPTION;
        }
        i++;
      }
      long multmin = limit / 10;
      long result = 0;
      while (i < end) {
        // Accumulating negatively avoids surprises near MAX_VALUE
        char c = cs.charAt(i++);
        if (c < '0' || c > '9' || result < multmin) {
          throw EXCEPTION;
        }

        int digit = c - '0';
        result *= 10;
        if (result < limit + digit) {
          throw EXCEPTION;
        }
        result -= digit;
      }
      return negative ? result : -result;
    } else {
      throw EXCEPTION;
    }
  }

  /**
   * Parses input accepting regular long syntax plus:
   * 1E1 -> 10
   * 1.234 -> 1
   * 1.123E1 -> 11
   * @param cs - char sequence to parse
   * @return parsed long value
   */
  public static long parseJsonLong(CharSequence cs) {
    if (cs == null) {
      throw NULL_EXCEPTION;
    }

    boolean negative = false;
    int i = 0;
    int len = cs.length();
    long limit = -Long.MAX_VALUE;

    if (len > 0) {
      boolean dotFound = false;
      boolean exponentFound = false;

      char firstChar = cs.charAt(0);
      if (firstChar < '0') { // Possible leading "+" or "-"
        if (firstChar == '-') {
          negative = true;
          limit = Long.MIN_VALUE;
        } else if (firstChar != '+') {
          throw EXCEPTION;
        }

        if (len == 1) { // Cannot have lone "+" or "-"
          throw EXCEPTION;
        }
        i++;
      }
      long multmin = limit / 10;
      long result = 0;
      while (i < len) {
        // Accumulating negatively avoids surprises near MAX_VALUE
        char c = cs.charAt(i++);
        if (c < '0' || c > '9' || result < multmin) {
          if (c == '.') {
            //ignore the rest of sequence
            dotFound = true;
            break;
          } else if (c == 'e' || c == 'E') {
            exponentFound = true;
            break;
          }
          throw EXCEPTION;
        }

        int digit = c - '0';
        result *= 10;
        if (result < limit + digit) {
          throw EXCEPTION;
        }
        result -= digit;
      }

      if (dotFound) {
        //scan rest of the string to make sure it's only digits
        while (i < len) {
          char c = cs.charAt(i++);
          if (c < '0' || c > '9') {
            if ((c | 32) == 'e') {
              exponentFound = true;
              break;
            } else {
              throw EXCEPTION;
            }
          }
        }
      }

      if (exponentFound) {
        if (dotFound) { // TODO: remove toString()
          return (long) Double.parseDouble(cs.toString());
        }

        long exp;
        try {
          exp = parseLong(cs, i, len);
        } catch (NumberFormatException nfe) {
          throw EXP_EXCEPTION;
        }

        if (exp < 0 || exp > POWERS_OF_10.length) {
          throw EXP_EXCEPTION;
        }

        return (negative ? result : -result) * POWERS_OF_10[(int) exp];
      }

      return negative ? result : -result;
    } else {
      throw EXCEPTION;
    }
  }
}
