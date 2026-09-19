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
package org.apache.pinot.common.utils;

import java.math.BigDecimal;


/// Shared JSON-number parser used by `jsonExtractScalar` (scalar and transform).
///
/// Accepts regular long syntax plus JSON numeric forms: `1E1` → `10`, `1.9` → `1` (truncate toward
/// zero), `1.123E1` → `11`. Dotted-exponent values are parsed as an exact decimal ([BigDecimal]),
/// truncated toward zero, then range-checked, so `9.223372036854775807E18` is [Long#MAX_VALUE] and
/// `9007199254740993.0E0` keeps the bit that a `double` would drop. Throws [NumberFormatException]
/// with `For input string: "<value>"` on overflow (`9223372036854775808`, `2.0E19`), illegal
/// exponent (`2E20`, `2E-1`), and other malformed input.
///
/// Thread-safe: no mutable state.
public final class JsonNumberUtils {
  private static final BigDecimal LONG_MIN_MINUS_ONE = new BigDecimal("-9223372036854775809");
  private static final BigDecimal LONG_MAX_PLUS_ONE = new BigDecimal("9223372036854775808");

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

  private JsonNumberUtils() {
  }

  /// Parses a JSON numeric string to a long.
  ///
  /// @param cs char sequence to parse
  /// @return parsed long value
  /// @throws NumberFormatException if `cs` is null, empty, out of long range, or not a JSON number
  public static long parseJsonLong(CharSequence cs) {
    if (cs == null) {
      throw new NumberFormatException("Can't parse null string");
    }

    boolean negative = false;
    int i = 0;
    int len = cs.length();
    long limit = -Long.MAX_VALUE;

    if (len <= 0) {
      throw formatException(cs);
    }

    boolean dotFound = false;
    boolean exponentFound = false;

    char firstChar = cs.charAt(0);
    if (firstChar < '0') { // Possible leading "+" or "-"
      if (firstChar == '-') {
        negative = true;
        limit = Long.MIN_VALUE;
      } else if (firstChar != '+') {
        throw formatException(cs);
      }

      if (len == 1) { // Cannot have lone "+" or "-"
        throw formatException(cs);
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
          // ignore the rest of the integer digits
          dotFound = true;
          break;
        } else if (c == 'e' || c == 'E') {
          exponentFound = true;
          break;
        }
        throw formatException(cs);
      }

      int digit = c - '0';
      result *= 10;
      if (result < limit + digit) {
        throw formatException(cs);
      }
      result -= digit;
    }

    if (dotFound) {
      // scan rest of the string to make sure it's only digits (or an exponent)
      while (i < len) {
        char c = cs.charAt(i++);
        if (c < '0' || c > '9') {
          if ((c | 32) == 'e') {
            exponentFound = true;
            break;
          } else {
            throw formatException(cs);
          }
        }
      }
    }

    if (exponentFound) {
      if (dotFound) {
        // Do not go through double: 9.223372036854775807E18 is Long.MAX_VALUE, but
        // Double.parseDouble rounds it to 2^63, which is out of range.
        return parseExactLong(cs);
      }

      long exp;
      try {
        exp = parseWholeLong(cs, i, len);
      } catch (NumberFormatException nfe) {
        throw new NumberFormatException("Wrong exponent");
      }

      if (exp < 0 || exp >= POWERS_OF_10.length) {
        throw new NumberFormatException("Wrong exponent");
      }

      try {
        return Math.multiplyExact(negative ? result : -result, POWERS_OF_10[(int) exp]);
      } catch (ArithmeticException e) {
        throw formatException(cs);
      }
    }

    return negative ? result : -result;
  }

  /// Parses a JSON numeric string as an exact decimal, truncates toward zero, then requires a long.
  private static long parseExactLong(CharSequence cs) {
    try {
      return truncateToLong(new BigDecimal(cs.toString()));
    } catch (NumberFormatException e) {
      throw formatException(cs);
    }
  }

  /// Truncates an exact decimal toward zero and requires the result to fit in a long.
  ///
  /// The comparisons happen before conversion so a value such as `1E999999999` is rejected
  /// without expanding its exponent into an enormous integer. Values strictly
  /// between `Long.MIN_VALUE - 1` and `Long.MAX_VALUE + 1` truncate into the long range.
  public static long truncateToLong(BigDecimal value) {
    if (value.compareTo(LONG_MIN_MINUS_ONE) <= 0 || value.compareTo(LONG_MAX_PLUS_ONE) >= 0) {
      throw formatException(value.toString());
    }
    return value.longValue();
  }

  /// Parses `cs[start, end)` as a whole long (sign allowed). Used for the exponent field.
  private static long parseWholeLong(CharSequence cs, int start, int end) {
    if (cs == null) {
      throw new NumberFormatException("Can't parse null string");
    }

    boolean negative = false;
    int i = start;
    long limit = -Long.MAX_VALUE;

    if (end <= start) {
      throw formatException(cs);
    }

    char firstChar = cs.charAt(start);
    if (firstChar < '0') { // Possible leading "+" or "-"
      if (firstChar == '-') {
        negative = true;
        limit = Long.MIN_VALUE;
      } else if (firstChar != '+') {
        throw formatException(cs);
      }

      if (end == start + 1) { // Cannot have lone "+" or "-"
        throw formatException(cs);
      }
      i++;
    }
    long multmin = limit / 10;
    long result = 0;
    while (i < end) {
      char c = cs.charAt(i++);
      if (c < '0' || c > '9' || result < multmin) {
        throw formatException(cs);
      }

      int digit = c - '0';
      result *= 10;
      if (result < limit + digit) {
        throw formatException(cs);
      }
      result -= digit;
    }
    return negative ? result : -result;
  }

  private static NumberFormatException formatException(CharSequence cs) {
    return new NumberFormatException("For input string: \"" + cs + "\"");
  }
}
