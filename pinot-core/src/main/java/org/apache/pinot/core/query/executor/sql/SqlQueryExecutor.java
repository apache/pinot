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
package org.apache.pinot.core.query.executor.sql;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.minion.MinionTaskUtils;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.sql.parsers.dml.DataManipulationStatement;
import org.apache.pinot.sql.parsers.dml.DataManipulationStatementParser;


/**
 * SqlQueryExecutor executes all SQL queries including DQL, DML, DCL, DDL.
 *
 */
public class SqlQueryExecutor {

  private HelixManager _helixManager;

  public SqlQueryExecutor(HelixManager helixManager) {
    _helixManager = helixManager;
  }

  /**
   * Execute DML Statement
   *
   * @param sqlNodeAndOptions
   * @param headers extra headers map for minion task submission
   * @return BrokerResponse
   * @throws IOException
   */
  public BrokerResponse executeDMLStatement(SqlNodeAndOptions sqlNodeAndOptions, Map<String, String> headers) {
    DataManipulationStatement statement = DataManipulationStatementParser.parse(sqlNodeAndOptions);
    BrokerResponseNative result = new BrokerResponseNative();
    switch (statement.getExecutionType()) {
      case MINION:
        AdhocTaskConfig taskConf = statement.generateAdhocTaskConfig();
        try {
          String response = MinionTaskUtils.executeTask(_helixManager, taskConf, headers);
          Map<String, String> tableToTaskIdMap = JsonUtils.stringToObject(response, Map.class);
          List<Object[]> rows = new ArrayList<>();
          tableToTaskIdMap.entrySet().forEach(entry -> rows.add(new Object[]{entry.getKey(), entry.getValue()}));
          result.setResultTable(new ResultTable(statement.getResultSchema(), rows));
        } catch (IOException e) {
          result.setExceptions(ImmutableList.of(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e)));
        }
        break;
      case HTTP:
        try {
          result.setResultTable(new ResultTable(statement.getResultSchema(), statement.execute()));
        } catch (Exception e) {
          result.setExceptions(ImmutableList.of(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e)));
        }
        break;
      default:
        result.setExceptions(ImmutableList.of(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR,
            new UnsupportedOperationException("Unsupported statement - " + statement))));
        break;
    }
    return result;
  }
}
