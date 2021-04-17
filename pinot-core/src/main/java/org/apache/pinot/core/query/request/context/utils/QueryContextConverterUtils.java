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
package org.apache.pinot.core.query.request.context.utils;

import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;


public class QueryContextConverterUtils {
  private QueryContextConverterUtils() {
  }

  private static final Pql2Compiler PQL_COMPILER = new Pql2Compiler();
  private static final CalciteSqlCompiler SQL_COMPILER = new CalciteSqlCompiler();

  /**
   * Converts the given PQL query into a {@link QueryContext}.
   */
  public static QueryContext getQueryContextFromPQL(String pqlQuery) {
    return BrokerRequestToQueryContextConverter.convert(PQL_COMPILER.compileToBrokerRequest(pqlQuery));
  }

  /**
   * Converts the given SQL query into a {@link QueryContext}.
   */
  public static QueryContext getQueryContextFromSQL(String sqlQuery) {
    return BrokerRequestToQueryContextConverter.convert(SQL_COMPILER.compileToBrokerRequest(sqlQuery));
  }
}
