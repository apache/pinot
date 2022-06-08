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
package org.apache.pinot.query.parser;

import java.util.List;
import java.util.regex.Pattern;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.sql.parsers.SqlCompilationException;


/**
 * Utility provided to Calcite parser.
 *
 * <p>This class is extracted from {@link org.apache.pinot.sql.parsers.CalciteSqlParser} for its static constructs.
 */
final class ParserUtils {
  /** Lexical policy similar to MySQL with ANSI_QUOTES option enabled. (To be
   * precise: MySQL on Windows; MySQL on Linux uses case-sensitive matching,
   * like the Linux file system.) The case of identifiers is preserved whether
   * or not they quoted; after which, identifiers are matched
   * case-insensitively. Double quotes allow identifiers to contain
   * non-alphanumeric characters. */
  static final Lex PINOT_LEX = Lex.MYSQL_ANSI;

  // BABEL is a very liberal conformance value that allows anything supported by any dialect
  static final SqlParser.Config PARSER_CONFIG =
      SqlParser.configBuilder().setLex(PINOT_LEX).setConformance(SqlConformanceEnum.BABEL)
          .setParserFactory(SqlBabelParserImpl.FACTORY).build();

  // TODO: move this to use parser syntax extension.
  // To Keep the backward compatibility with 'OPTION' Functionality in PQL, which is used to
  // provide more hints for query processing.
  //
  // PQL syntax is: `OPTION (<key> = <value>)`
  //
  // Multiple OPTIONs is also supported by:
  // either
  //   `OPTION (<k1> = <v1>, <k2> = <v2>, <k3> = <v3>)`
  // or
  //   `OPTION (<k1> = <v1>) OPTION (<k2> = <v2>) OPTION (<k3> = <v3>)`
  static final Pattern OPTIONS_REGEX_PATTEN = Pattern.compile("option\\s*\\(([^\\)]+)\\)", Pattern.CASE_INSENSITIVE);

  private ParserUtils() {
    // do not instantiate.
  }

  public static String canonicalize(String functionName) {
    return StringUtils.remove(functionName, '_').toLowerCase();
  }

  public static boolean isSameFunction(String function1, String function2) {
    return canonicalize(function1).equals(canonicalize(function2));
  }

  public static void validateFunction(String functionName, List<Expression> operands) {
    switch (canonicalize(functionName)) {
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

  private static void validateJsonExtractScalarFunction(List<Expression> operands) {
    int numOperands = operands.size();

    // Check that there are exactly 3 or 4 arguments
    if (numOperands != 3 && numOperands != 4) {
      throw new SqlCompilationException(
          "Expect 3 or 4 arguments for transform function: jsonExtractScalar(jsonFieldName, 'jsonPath', "
              + "'resultsType', ['defaultValue'])");
    }
    if (!operands.get(1).isSetLiteral() || !operands.get(2).isSetLiteral() || (numOperands == 4 && !operands.get(3)
        .isSetLiteral())) {
      throw new SqlCompilationException(
          "Expect the 2nd/3rd/4th argument of transform function: jsonExtractScalar(jsonFieldName, 'jsonPath',"
              + " 'resultsType', ['defaultValue']) to be a single-quoted literal value.");
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
