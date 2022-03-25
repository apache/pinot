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

import java.util.regex.Pattern;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;


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
}
