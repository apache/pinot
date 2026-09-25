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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.pinot.common.helix.ExtraInstanceConfig;
import org.apache.pinot.common.minion.MinionClient;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.sql.parsers.dml.DataManipulationStatement;
import org.apache.pinot.sql.parsers.dml.DataManipulationStatementParser;
import org.apache.pinot.sql.parsers.dml.DeleteStatement;


/// SqlQueryExecutor executes all SQL queries including DQL, DML, DCL, DDL.
public class SqlQueryExecutor {
  public static final String UNAUTHORIZED_DELETE_MESSAGE = "DELETE is only executed once its table is resolved and "
      + "the caller authorized to delete rows from it: send it to the query endpoint of a broker or a controller";

  private final String _controllerUrl;
  private final HelixManager _helixManager;

  /// Fetch the lead controller from helix, HA is not guaranteed.
  /// @param helixManager is used to query leader controller from helix.
  public SqlQueryExecutor(HelixManager helixManager) {
    _helixManager = helixManager;
    _controllerUrl = null;
  }

  /// Recommended to provide the controller vip or service name for access.
  /// @param controllerUrl controller service name for sending minion task requests
  public SqlQueryExecutor(String controllerUrl) {
    _controllerUrl = controllerUrl;
    _helixManager = null;
  }

  /// Base URL of the controller that executes the DML statements: the configured controller URL, or else the current
  /// lead controller, looked up on each call.
  protected String getControllerBaseUrl() {
    if (_helixManager == null) {
      return _controllerUrl;
    }
    String instanceId = LeadControllerUtils.getHelixClusterLeader(_helixManager);
    if (instanceId == null) {
      throw new RuntimeException("Unable to locate the leader pinot controller, please retry later...");
    }

    HelixDataAccessor helixDataAccessor = _helixManager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = helixDataAccessor.keyBuilder();
    ExtraInstanceConfig extraInstanceConfig = new ExtraInstanceConfig(helixDataAccessor.getProperty(
        keyBuilder.instanceConfig(CommonConstants.Helix.PREFIX_OF_CONTROLLER_INSTANCE + instanceId)));
    String controllerBaseUrl = extraInstanceConfig.getComponentUrl();
    if (controllerBaseUrl == null) {
      throw new RuntimeException("Unable to extract the base url from the leader pinot controller");
    }
    return controllerBaseUrl;
  }

  /// Parses and executes a DML statement.
  ///
  /// A `DELETE` is refused with a [QueryErrorCode#ACCESS_DENIED] error, since this method cannot authorize the caller:
  /// it is executed with [#executeStatement] once its table is resolved and the caller authorized to delete rows from
  /// it, as the query endpoints of the broker and the controller do.
  ///
  /// @param sqlNodeAndOptions Parsed DML object
  /// @param headers extra headers map for minion task submission
  /// @return BrokerResponse is the DML executed response
  public BrokerResponse executeDMLStatement(SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable Map<String, String> headers) {
    DataManipulationStatement statement;
    try {
      statement = DataManipulationStatementParser.parse(sqlNodeAndOptions);
    } catch (QueryException e) {
      // e.g. a DELETE without a WHERE clause, or a DML kind that Pinot parses but does not execute (UPDATE, MERGE)
      return new BrokerResponseNative(e.getErrorCode(), e.getMessage());
    }
    if (statement instanceof DeleteStatement) {
      return new BrokerResponseNative(QueryErrorCode.ACCESS_DENIED, UNAUTHORIZED_DELETE_MESSAGE);
    }
    return executeStatement(statement, headers);
  }

  /// Executes a parsed DML statement, e.g. from [DataManipulationStatementParser#parse].
  ///
  /// It does not authorize the caller. The table of a [DeleteStatement] must be resolved with
  /// [DeleteStatement#resolveTableName] and the caller authorized to delete rows from it before it is executed, as the
  /// query endpoints of the broker and the controller do: an unresolved `DELETE` is refused with a
  /// [QueryErrorCode#ACCESS_DENIED] error.
  ///
  /// @param statement parsed statement
  /// @param headers headers of the original request, e.g. for minion task submission
  /// @return the response of the statement
  public BrokerResponse executeStatement(DataManipulationStatement statement, @Nullable Map<String, String> headers) {
    if (statement instanceof DeleteStatement) {
      DeleteStatement deleteStatement = (DeleteStatement) statement;
      if (!deleteStatement.isResolved()) {
        return new BrokerResponseNative(QueryErrorCode.ACCESS_DENIED, UNAUTHORIZED_DELETE_MESSAGE);
      }
      return executeDelete(deleteStatement, headers);
    }
    BrokerResponseNative result = new BrokerResponseNative();
    switch (statement.getExecutionType()) {
      case MINION:
        AdhocTaskConfig taskConf = statement.generateAdhocTaskConfig();
        try {
          Map<String, String> tableToTaskIdMap = getMinionClient().executeTask(taskConf, headers);
          List<Object[]> rows = new ArrayList<>();
          tableToTaskIdMap.forEach((key, value) -> rows.add(new Object[]{key, value}));
          result.setResultTable(new ResultTable(statement.getResultSchema(), rows));
        } catch (Exception e) {
          result.addException(new QueryProcessingException(QueryErrorCode.QUERY_EXECUTION, e.getMessage()));
        }
        break;
      case HTTP:
        try {
          result.setResultTable(new ResultTable(statement.getResultSchema(), statement.execute()));
        } catch (Exception e) {
          result.addException(new QueryProcessingException(QueryErrorCode.QUERY_EXECUTION, e.getMessage()));
        }
        break;
      default:
        result.addException(
            new QueryProcessingException(QueryErrorCode.QUERY_EXECUTION, "Unsupported statement: " + statement));
        break;
    }
    return result;
  }

  /// Executes a `DELETE` statement. Pinot does not delete rows itself, so this implementation answers with a
  /// [QueryErrorCode#QUERY_VALIDATION] error: executors that implement row deletion override it.
  ///
  /// The query endpoints of the broker and the controller call it once the caller is authorized to delete rows from
  /// the table of the statement, resolved with [DeleteStatement#resolveTableName]: implementations delete rows from
  /// that exact table, [DeleteStatement#getTableName()]. Both require the checks of a query on the table, since the
  /// WHERE clause reads it, plus the right to delete rows: on the broker, `AccessControl#authorizeDeleteRows`, which
  /// denies by default, and no row-level security filter on the table; on the controller, the `DELETE` access type
  /// and the `DeleteRows` table action (`Actions.Table#DELETE_ROWS`). Neither applies quotas nor logs the statement
  /// as a query.
  ///
  /// Implementations validate the predicate (see [DeleteStatement#getPredicate()]) and the options they read (see
  /// [DeleteStatement#getOptions()]) before deleting rows, and forward the request headers to the APIs they call, which
  /// authorize the caller again.
  ///
  /// @param statement parsed statement, with its table resolved
  /// @param headers headers of the original request, e.g. to authorize the caller
  /// @return the response of the statement
  protected BrokerResponse executeDelete(DeleteStatement statement, @Nullable Map<String, String> headers) {
    return new BrokerResponseNative(QueryErrorCode.QUERY_VALIDATION, DeleteStatement.NOT_SUPPORTED_MESSAGE);
  }

  private MinionClient getMinionClient() {
    // NOTE: using null auth provider here as auth headers injected by caller in "executeStatement()"
    return new MinionClient(getControllerBaseUrl(), null);
  }
}
