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
   * Sanitize the sql string for parsing by normalizing whitespace
   * which is likely to cause performance issues with regex parsing.
   * @param sql string to sanitize
   * @return sanitized sql string
   */
  public static String sanitizeSql(String sql) {

    // 1. Remove trailing whitespaces

    int endIndex = sql.length() - 1;
    while (endIndex >= 0 && Character.isWhitespace(sql.charAt(endIndex))) {
      endIndex--;
    }
    sql = sql.substring(0, endIndex + 1);

    // Likewise extend for other improvements

    return sql;
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
