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
package org.apache.pinot.core.requesthandler;

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.parsers.QueryCompiler;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;

import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.PQL;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.SQL;


public class PinotQueryParserFactory {
  private PinotQueryParserFactory() {
  }

  private static final Pql2Compiler PQL_2_COMPILER = new Pql2Compiler();
  private static final CalciteSqlCompiler CALCITE_SQL_COMPILER = new CalciteSqlCompiler();

  public static QueryCompiler get(String queryFormat) {
    switch (queryFormat.toLowerCase()) {
      case PQL:
        return PQL_2_COMPILER;
      case SQL:
        return CALCITE_SQL_COMPILER;
      default:
        throw new UnsupportedOperationException("Unknown query format - " + queryFormat);
    }
  }

  public static BrokerRequest parseSQLQuery(String query) {
    return CALCITE_SQL_COMPILER.compileToBrokerRequest(query);
  }

  public static BrokerRequest parsePQLQuery(String query) {
    return PQL_2_COMPILER.compileToBrokerRequest(query);
  }
}
