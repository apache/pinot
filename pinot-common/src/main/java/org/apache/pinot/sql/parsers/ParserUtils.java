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
package org.apache.pinot.sql.parsers;

import java.util.List;
import org.apache.pinot.common.request.Expression;


public class ParserUtils {
  private ParserUtils() {
  }

  public static void validateFunction(String canonicalName, List<Expression> operands) {
    switch (canonicalName) {
      case "jsonextractscalar":
        validateJsonExtractScalarFunction(operands);
        break;
      case "jsonextractkey":
        validateJsonExtractKeyFunction(operands);
        break;
      default:
        break;
    }
  }

  /**
   * Sanitize the sql string for parsing by normalizing whitespace which can
   * cause performance issues with regex based parsing.
   * This method replaces multiple consecutive whitespace characters with a single space.
   *
   * @param sql The raw SQL string to sanitize. May be null.
   * @return A sanitized SQL string with normalized whitespace and no trailing spaces,
   *         or {@code null} if the input was {@code null}.
   */
  public static String sanitizeSqlForParsing(String sql) {

    // 1. Remove excessive whitespace

    int length = sql.length();
    StringBuilder builder = new StringBuilder(length);
    boolean inWhitespaceBlock = false;

    for (int charIndex = 0; charIndex < length; charIndex++) {
      char currentChar = sql.charAt(charIndex);

      if (Character.isWhitespace(currentChar)) {
        if (currentChar == '\n' || currentChar == '\r') {
          builder.append(currentChar); // preserve line breaks
          inWhitespaceBlock = false; // reset space block
        } else if (!inWhitespaceBlock) {
          builder.append(' ');
          inWhitespaceBlock = true;
        }
      } else {
        builder.append(currentChar);
        inWhitespaceBlock = false; // reset space block
      }
    }

    if (sql.length() == builder.length()) {
      return sql; // No excessive whitespace found
    }

    // Likewise extend for other improvements

    return builder.toString();
  }

  private static void validateJsonExtractScalarFunction(List<Expression> operands) {
    // Check that there are 3 or 4 arguments
    int numOperands = operands.size();
    if (numOperands != 3 && numOperands != 4) {
      throw new SqlCompilationException(
          "Expect 3 or 4 arguments for transform function: jsonExtractScalar(jsonFieldName, 'jsonPath', "
              + "'resultsType', ['defaultValue'])");
    }
    if (!operands.get(1).isSetLiteral() || !operands.get(2).isSetLiteral() || (numOperands == 4 && !operands.get(3)
        .isSetLiteral())) {
      throw new SqlCompilationException(
          "Expect the 2nd/3rd/4th argument of transform function: jsonExtractScalar(jsonFieldName, 'jsonPath', "
              + "'resultsType', ['defaultValue']) to be a single-quoted literal value.");
    }
  }

  private static void validateJsonExtractKeyFunction(List<Expression> operands) {
    // Check that there are exactly 2 arguments
    if (operands.size() != 2) {
      throw new SqlCompilationException(
          "Expect 2 arguments are required for transform function: jsonExtractKey(jsonFieldName, 'jsonPath')");
    }
    if (!operands.get(1).isSetLiteral()) {
      throw new SqlCompilationException(
          "Expect the 2nd argument for transform function: jsonExtractKey(jsonFieldName, 'jsonPath') to be a "
              + "single-quoted literal value.");
    }
  }
}
