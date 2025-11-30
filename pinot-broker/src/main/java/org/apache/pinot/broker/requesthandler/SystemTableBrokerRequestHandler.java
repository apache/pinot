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
package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.querylog.QueryLogger;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.systemtable.SystemTableDataProvider;
import org.apache.pinot.common.systemtable.SystemTableRegistry;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker request handler for system tables (handled entirely on the broker).
 */
public class SystemTableBrokerRequestHandler extends BaseBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SystemTableBrokerRequestHandler.class);

  private final SystemTableRegistry _systemTableRegistry;

  public SystemTableBrokerRequestHandler(PinotConfiguration config, String brokerId,
      BrokerRequestIdGenerator requestIdGenerator, RoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      SystemTableRegistry systemTableRegistry, ThreadAccountant threadAccountant) {
    super(config, brokerId, requestIdGenerator, routingManager, accessControlFactory, queryQuotaManager, tableCache,
        threadAccountant);
    _systemTableRegistry = systemTableRegistry;
  }

  @Override
  public void start() {
  }

  @Override
  public void shutDown() {
  }

  public boolean canHandle(String tableName) {
    return isSystemTable(tableName) && _systemTableRegistry.isRegistered(tableName);
  }

  @Override
  protected BrokerResponse handleRequest(long requestId, String query, SqlNodeAndOptions sqlNodeAndOptions,
      JsonNode request, @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      @Nullable HttpHeaders httpHeaders, AccessControl accessControl)
      throws Exception {
    PinotQuery pinotQuery;
    try {
      pinotQuery = CalciteSqlParser.compileToPinotQuery(sqlNodeAndOptions);
    } catch (Exception e) {
      requestContext.setErrorCode(QueryErrorCode.SQL_PARSING);
      return new BrokerResponseNative(QueryErrorCode.SQL_PARSING, e.getMessage());
    }

    Set<String> tableNames = RequestUtils.getTableNames(pinotQuery);
    if (tableNames == null || tableNames.isEmpty()) {
      requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
      return new BrokerResponseNative(QueryErrorCode.QUERY_VALIDATION, "Failed to extract table name");
    }
    if (tableNames.size() != 1) {
      requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
      return new BrokerResponseNative(QueryErrorCode.QUERY_VALIDATION, "System tables do not support joins");
    }
    String tableName = tableNames.iterator().next();
    if (!isSystemTable(tableName)) {
      requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
      return new BrokerResponseNative(QueryErrorCode.QUERY_VALIDATION, "Not a system table query");
    }
    AuthorizationResult authorizationResult =
        hasTableAccess(requesterIdentity, Set.of(tableName), requestContext, httpHeaders);
    if (!authorizationResult.hasAccess()) {
      requestContext.setErrorCode(QueryErrorCode.ACCESS_DENIED);
      return new BrokerResponseNative(QueryErrorCode.ACCESS_DENIED, authorizationResult.getFailureMessage());
    }

    return handleSystemTableQuery(pinotQuery, tableName, requestContext, requesterIdentity, query);
  }

  @Override
  protected boolean handleCancel(long queryId, int timeoutMs, Executor executor,
      HttpClientConnectionManager connMgr, Map<String, Integer> serverResponses) {
    return false;
  }

  @Override
  public boolean cancelQueryByClientId(String clientQueryId, int timeoutMs, Executor executor,
      HttpClientConnectionManager connMgr, Map<String, Integer> serverResponses)
      throws Exception {
    return false;
  }

  @Override
  public Map<Long, String> getRunningQueries() {
    return Collections.emptyMap();
  }

  @Override
  public OptionalLong getRequestIdByClientId(String clientQueryId) {
    return OptionalLong.empty();
  }

  private boolean isSystemTable(String tableName) {
    return tableName != null && tableName.toLowerCase(Locale.ROOT).startsWith("system.");
  }

  private BrokerResponse handleSystemTableQuery(PinotQuery pinotQuery, String tableName, RequestContext requestContext,
      @Nullable RequesterIdentity requesterIdentity, String query) {
    if (pinotQuery.isExplain()) {
      return BrokerResponseNative.BROKER_ONLY_EXPLAIN_PLAN_OUTPUT;
    }
    SystemTableDataProvider provider = _systemTableRegistry.get(tableName);
    if (provider == null) {
      requestContext.setErrorCode(QueryErrorCode.TABLE_DOES_NOT_EXIST);
      return BrokerResponseNative.TABLE_DOES_NOT_EXIST;
    }
    try {
      if (!isSupportedSystemTableQuery(pinotQuery)) {
        requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
        return new BrokerResponseNative(QueryErrorCode.QUERY_VALIDATION,
            "System tables only support simple projection/filter/limit queries");
      }
      BrokerResponseNative brokerResponse = provider.getBrokerResponse(pinotQuery);
      if (CollectionUtils.isEmpty(brokerResponse.getTablesQueried())) {
        brokerResponse.setTablesQueried(Set.of(TableNameBuilder.extractRawTableName(tableName)));
      }
      brokerResponse.setTimeUsedMs(System.currentTimeMillis() - requestContext.getRequestArrivalTimeMillis());
      _queryLogger.log(new QueryLogger.QueryLogParams(requestContext, tableName, brokerResponse,
          QueryLogger.QueryLogParams.QueryEngine.SINGLE_STAGE, requesterIdentity, null));
      return brokerResponse;
    } catch (BadQueryRequestException e) {
      requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERY_VALIDATION_EXCEPTIONS, 1);
      return new BrokerResponseNative(QueryErrorCode.QUERY_VALIDATION, e.getMessage());
    } catch (Exception e) {
      LOGGER.warn("Caught exception while handling system table query {}: {}", tableName, e.getMessage(), e);
      requestContext.setErrorCode(QueryErrorCode.QUERY_EXECUTION);
      return new BrokerResponseNative(QueryErrorCode.QUERY_EXECUTION, e.getMessage());
    }
  }

  private boolean isSupportedSystemTableQuery(PinotQuery pinotQuery) {
    return (pinotQuery.getGroupByList() == null || pinotQuery.getGroupByList().isEmpty())
        && pinotQuery.getHavingExpression() == null
        && (pinotQuery.getOrderByList() == null || pinotQuery.getOrderByList().isEmpty());
  }
}
