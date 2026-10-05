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

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.utils.SqlUtils;


public class ParserUtils {
  private ParserUtils() {
  }

  /// Converts a raw or SQL-quoted identifier into a safe SQL identifier fragment. Qualified identifiers are split on
  /// dots outside quoted components. Valid bare Pinot identifier components and wildcards remain bare, while other
  /// components are double-quoted. Backtick-quoted and double-quoted components are normalized to double quotes.
  ///
  /// @param identifier Raw or SQL-quoted identifier, optionally qualified
  /// @return Identifier formatted for use in a SQL statement
  /// @throws IllegalArgumentException If the identifier is empty or has invalid quoted structure
  public static String sanitizeIdentifier(String identifier) {
    String trimmed = identifier.trim();
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException("identifier cannot be empty");
    }
    if (trimmed.indexOf('.') < 0 && trimmed.indexOf('`') < 0 && trimmed.indexOf('"') < 0) {
      return sanitizeIdentifierSegment(trimmed);
    }

    StringJoiner result = new StringJoiner(".");
    for (String segment : splitIdentifier(trimmed)) {
      result.add(sanitizeIdentifierSegment(segment));
    }
    return result.toString();
  }

  /// Validates and normalizes an aggregation function name for use in a generated SQL statement.
  ///
  /// @param aggregationFunction Aggregation function name
  /// @return Trimmed aggregation function name
  /// @throws IllegalArgumentException If the function is not a registered Pinot aggregation function
  public static String sanitizeAggregationFunction(String aggregationFunction) {
    String trimmed = aggregationFunction.trim();
    if (!AggregationFunctionType.isAggregationFunction(trimmed)) {
      throw new IllegalArgumentException("Invalid aggregation function: " + trimmed);
    }
    return trimmed;
  }

  /// Validates a predicate for use in a generated SQL statement. Statement separators and comments outside quoted
  /// content are rejected, and the predicate must compile as a Pinot SQL expression.
  ///
  /// @param predicate Predicate expression, or `null`
  /// @return Trimmed predicate, or `null` when the input is null or blank
  /// @throws IllegalArgumentException If the predicate contains blocked SQL content or is not a Pinot filter
  @Nullable
  public static String sanitizePredicate(@Nullable String predicate) {
    if (predicate == null || predicate.trim().isEmpty()) {
      return null;
    }
    String trimmed = predicate.trim();
    if (containsDangerousPredicateContent(trimmed)) {
      throw new IllegalArgumentException("Invalid predicate content");
    }
    try {
      CalciteSqlParser.compileToExpression(trimmed);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Invalid predicate content", e);
    }
    return trimmed;
  }

  public static void validateFunction(String canonicalName, List<Expression> operands) {
    switch (canonicalName) {
      case "jsonextractscalar":
        validateJsonExtractScalarFunction("jsonExtractScalar", operands);
        break;
      case "jsonextractscalarfast":
        validateJsonExtractScalarFunction("jsonExtractScalarFast", operands);
        break;
      case "jsonextractscalarfirstmatch":
        validateJsonExtractScalarFunction("jsonExtractScalarFirstMatch", operands);
        break;
      case "jsonextractscalarfory":
        validateJsonExtractScalarFunction("jsonExtractScalarFory", operands);
        break;
      case "jsonextractkey":
        validateJsonExtractKeyFunction(operands);
        break;
      default:
        break;
    }
  }

  /// Sanitize the sql string for parsing by normalizing whitespace
  /// which is likely to cause performance issues with regex parsing.
  /// @param sql string to sanitize
  /// @return sanitized sql string
  public static String sanitizeSql(String sql) {

    // 1. Strip single-line SQL comments (-- ... to end of line).
    // The legacy OPTIONS regex anchors at end-of-string, so without this step a query
    // like "SELECT col1 FROM foo -- option(skipUpsert=true)" would be mistakenly matched
    // as if skipUpsert were a real query option.
    sql = stripSingleLineComments(sql);

    // 2. Remove trailing whitespace
    int endIndex = sql.length() - 1;
    while (endIndex >= 0 && Character.isWhitespace(sql.charAt(endIndex))) {
      endIndex--;
    }
    return sql.substring(0, endIndex + 1);
  }

  /// Returns the sql string with all single-line SQL comments (-- ... to end of line) removed,
  /// respecting single-quoted string literals, double-quoted identifiers, and block comments.
  /// A "--" found inside a block comment or a quoted context is not treated as a comment marker.
  static String stripSingleLineComments(String sql) {
    StringBuilder result = new StringBuilder(sql.length());
    int len = sql.length();
    boolean inSingleQuote = false;
    boolean inDoubleQuote = false;
    boolean inBlockComment = false;
    int i = 0;
    while (i < len) {
      char c = sql.charAt(i);
      if (inBlockComment) {
        result.append(c);
        if (c == '*' && i + 1 < len && sql.charAt(i + 1) == '/') {
          result.append('/');
          inBlockComment = false;
          i += 2;
        } else {
          i++;
        }
      } else if (inSingleQuote) {
        result.append(c);
        if (c == '\'' && i + 1 < len && sql.charAt(i + 1) == '\'') {
          result.append('\'');
          i += 2; // '' escape inside a single-quoted literal
        } else {
          if (c == '\'') {
            inSingleQuote = false;
          }
          i++;
        }
      } else if (inDoubleQuote) {
        result.append(c);
        if (c == '"' && i + 1 < len && sql.charAt(i + 1) == '"') {
          result.append('"');
          i += 2; // "" escape inside a double-quoted identifier
        } else {
          if (c == '"') {
            inDoubleQuote = false;
          }
          i++;
        }
      } else {
        if (c == '\'') {
          inSingleQuote = true;
          result.append(c);
          i++;
        } else if (c == '"') {
          inDoubleQuote = true;
          result.append(c);
          i++;
        } else if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
          inBlockComment = true;
          result.append(c);
          i++;
        } else if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
          // Skip from here to end of line; the newline itself is kept.
          while (i < len && sql.charAt(i) != '\n') {
            i++;
          }
        } else {
          result.append(c);
          i++;
        }
      }
    }
    return result.toString();
  }

  private static List<String> splitIdentifier(String identifier) {
    List<String> segments = new ArrayList<>();
    StringBuilder segment = new StringBuilder();
    char quote = 0;
    boolean segmentStarted = false;
    int index = 0;
    while (index < identifier.length()) {
      char current = identifier.charAt(index);
      if (quote == 0) {
        if (current == '.') {
          segments.add(segment.toString());
          segment.setLength(0);
          segmentStarted = false;
          index++;
          continue;
        }
        if (!segmentStarted && (current == '`' || current == '"')) {
          quote = current;
        }
        if (!Character.isWhitespace(current)) {
          segmentStarted = true;
        }
      } else if (current == quote) {
        if (index + 1 < identifier.length() && identifier.charAt(index + 1) == quote) {
          segment.append(current);
          segment.append(identifier.charAt(index + 1));
          index += 2;
          continue;
        }
        quote = 0;
      }
      segment.append(current);
      index++;
    }
    if (quote != 0) {
      throw new IllegalArgumentException("Unterminated quoted identifier");
    }
    segments.add(segment.toString());
    return segments;
  }

  private static String sanitizeIdentifierSegment(String segment) {
    String trimmed = segment.trim();
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException("Identifier segment cannot be empty");
    }
    if ("*".equals(trimmed)) {
      return trimmed;
    }
    if (trimmed.charAt(0) == '`' || trimmed.charAt(0) == '"') {
      return sanitizeQuotedIdentifierSegment(trimmed);
    }
    return sanitizeUnquotedIdentifierSegment(trimmed);
  }

  private static String sanitizeUnquotedIdentifierSegment(String segment) {
    try {
      Expression expression = CalciteSqlParser.compileToExpression(segment);
      if (expression.getType() == ExpressionType.IDENTIFIER
          && expression.isSetIdentifier()
          && segment.equals(expression.getIdentifier().getName())) {
        return segment;
      }
    } catch (SqlCompilationException ignored) {
      // Not a bare Pinot identifier; quote it below.
    }
    return SqlUtils.quoteIdentifier(segment);
  }

  private static String sanitizeQuotedIdentifierSegment(String segment) {
    char quote = segment.charAt(0);
    if (segment.length() < 2 || segment.charAt(segment.length() - 1) != quote) {
      throw new IllegalArgumentException("Invalid quoted identifier segment: " + segment);
    }

    StringBuilder unquoted = new StringBuilder(segment.length() - 2);
    int index = 1;
    while (index < segment.length() - 1) {
      char current = segment.charAt(index);
      if (current == quote) {
        if (index + 1 >= segment.length() - 1 || segment.charAt(index + 1) != quote) {
          throw new IllegalArgumentException("Invalid quoted identifier segment: " + segment);
        }
        index++;
      }
      unquoted.append(current);
      index++;
    }
    if (unquoted.length() == 0) {
      throw new IllegalArgumentException("Quoted identifier segment cannot be empty");
    }
    return SqlUtils.quoteIdentifier(unquoted.toString());
  }

  private static boolean containsDangerousPredicateContent(String predicate) {
    char quote = 0;
    int index = 0;
    while (index < predicate.length()) {
      char current = predicate.charAt(index);
      if (quote != 0) {
        if (current == quote) {
          if (index + 1 < predicate.length() && predicate.charAt(index + 1) == quote) {
            index += 2;
            continue;
          }
          quote = 0;
        }
        index++;
        continue;
      }

      if (current == '\'' || current == '"' || current == '`') {
        quote = current;
        index++;
        continue;
      }
      if (current == ';') {
        return true;
      }
      if (index + 1 < predicate.length()) {
        char next = predicate.charAt(index + 1);
        if ((current == '-' && next == '-')
            || (current == '/' && next == '*')
            || (current == '*' && next == '/')) {
          return true;
        }
      }
      index++;
    }
    return quote != 0;
  }

  private static void validateJsonExtractScalarFunction(String functionName, List<Expression> operands) {
    // Check that there are 3 or 4 arguments
    int numOperands = operands.size();
    if (numOperands != 3 && numOperands != 4) {
      throw new SqlCompilationException(
          "Expect 3 or 4 arguments for transform function: " + functionName
              + "(jsonFieldName, 'jsonPath', 'resultsType', ['defaultValue'])");
    }
    if (!operands.get(1).isSetLiteral() || !operands.get(2).isSetLiteral() || (numOperands == 4 && !operands.get(3)
        .isSetLiteral())) {
      throw new SqlCompilationException(
          "Expect the 2nd and 3rd arguments of transform function: " + functionName
              + "(jsonFieldName, 'jsonPath', 'resultsType', ['defaultValue']) to be single-quoted literal values, "
              + "and the optional 4th argument to be a literal value.");
    }
  }

  private static void validateJsonExtractKeyFunction(List<Expression> operands) {
    // Check that there are 2 or 3 arguments
    if (operands.size() < 2 || operands.size() > 3) {
      throw new SqlCompilationException(
          "2 or 3 arguments are required for transform function: "
              + "jsonExtractKey(jsonFieldName, 'jsonPath', [optionalParameters])");
    }
    if (!operands.get(1).isSetLiteral()) {
      throw new SqlCompilationException(
          "Expect the 2nd argument for transform function: "
              + "jsonExtractKey(jsonFieldName, 'jsonPath', [optionalParameters]) "
              + "to be a single-quoted literal value.");
    }
    // Note: 3rd argument (optionalParameters) should be a string literal
    if (operands.size() > 2 && !operands.get(2).isSetLiteral()) {
      throw new SqlCompilationException(
          "Expect the 3rd argument for transform function: "
              + "jsonExtractKey(jsonFieldName, 'jsonPath', [optionalParameters]) "
              + "to be a single-quoted literal value.");
    }
    // Runtime validation will ensure correct types
  }
}
