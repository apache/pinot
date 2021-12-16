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

import org.apache.pinot.query.context.PlannerContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.query.parser.ParserUtils.OPTIONS_REGEX_PATTEN;
import static org.apache.pinot.query.parser.ParserUtils.PARSER_CONFIG;


public class CalciteSqlParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(CalciteSqlParser.class);

  /**
   * entrypoint for Sql Parser.
   */
  public static SqlNode compile(String sql, PlannerContext plannerContext)
      throws SqlCompilationException {
    // Extract OPTION statements from sql as Calcite Parser doesn't parse it.
    // TODO: use parser syntax extension instead.
    Map<String, String> options = parseOptions(extractOptionsFromSql(sql));
    plannerContext.setOptions(options);
    if (!options.isEmpty()) {
      sql = removeOptionsFromSql(sql);
    }
    // Compile Sql without OPTION statements.
    SqlNode parsed = parse(sql);

    // query rewrite.
    return QueryRewriter.rewrite(parsed, plannerContext);
  }

  // ==========================================================================
  // Static utils to parse the SQL.
  // ==========================================================================

  private static Map<String, String> parseOptions(List<String> optionsStatements) {
    if (optionsStatements.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> options = new HashMap<>();
    for (String optionsStatement : optionsStatements) {
      for (String option : optionsStatement.split(",")) {
        final String[] splits = option.split("=");
        if (splits.length != 2) {
          throw new SqlCompilationException("OPTION statement requires two parts separated by '='");
        }
        options.put(splits[0].trim(), splits[1].trim());
      }
    }
    return options;
  }

  private static SqlNode parse(String sql) {
    SqlParser sqlParser = SqlParser.create(sql, PARSER_CONFIG);
    SqlNode sqlNode;
    try {
      sqlNode = sqlParser.parseQuery();
    } catch (SqlParseException e) {
      throw new SqlCompilationException("Caught exception while parsing query: " + sql, e);
    }

    // This is a special rewrite,
    // TODO: move it to planner later.
    SqlSelect selectNode;
    if (sqlNode instanceof SqlOrderBy) {
      // Store order-by info into the select sql node
      SqlOrderBy orderByNode = (SqlOrderBy) sqlNode;
      selectNode = (SqlSelect) orderByNode.query;
      selectNode.setOrderBy(orderByNode.orderList);
      selectNode.setFetch(orderByNode.fetch);
      selectNode.setOffset(orderByNode.offset);
    } else {
      selectNode = (SqlSelect) sqlNode;
    }
    return selectNode;
  }

  private static List<String> extractOptionsFromSql(String sql) {
    List<String> results = new ArrayList<>();
    Matcher matcher = OPTIONS_REGEX_PATTEN.matcher(sql);
    while (matcher.find()) {
      results.add(matcher.group(1));
    }
    return results;
  }

  private static String removeOptionsFromSql(String sql) {
    Matcher matcher = OPTIONS_REGEX_PATTEN.matcher(sql);
    return matcher.replaceAll("");
  }
}
