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
package org.apache.pinot.query.runtime.queries;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.runtime.operator.LeafOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.query.testutils.SlowMockInstanceDataManagerFactory;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration tests covering timeout overflow behavior end-to-end.
 */
public class TimeoutOverflowQueryRunnerTest extends QueryRunnerTestBase {
  private static final String TABLE_NAME = "slowTable";
  private static final String TABLE_NAME_WITH_TYPE = TABLE_NAME + "_OFFLINE";
  private static final String FAST_TABLE_NAME = "fastTable";
  private static final String FAST_TABLE_WITH_TYPE = FAST_TABLE_NAME + "_OFFLINE";
  private static final long LEAF_TIMEOUT_MS = 1;
  private static final long LEAF_EXTRA_PASSIVE_TIMEOUT_MS = 0;
  private static final long QUERY_TIMEOUT_MS = 2000;
  private static final long QUERY_EXTRA_PASSIVE_TIMEOUT_MS = 1000;

  private QueryServerEnclosure _slowServer;
  private QueryServerEnclosure _fastServer;

  @BeforeClass
  public void setUp()
      throws Exception {
    SlowMockInstanceDataManagerFactory slowFactory = new SlowMockInstanceDataManagerFactory("slowServer", 500);
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("col1", FieldSpec.DataType.STRING, "")
        .addSingleValueDimension("col2", FieldSpec.DataType.STRING, "")
        .addMetric("col3", FieldSpec.DataType.INT, 0)
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .setSchemaName(TABLE_NAME)
        .setEnableColumnBasedNullHandling(true)
        .build();
    slowFactory.registerTable(schema, TABLE_NAME_WITH_TYPE);
    slowFactory.addSegment(TABLE_NAME_WITH_TYPE, QueryRunnerTest.buildRows(TABLE_NAME_WITH_TYPE));
    slowFactory.addSegment(TABLE_NAME_WITH_TYPE, QueryRunnerTest.buildRows(TABLE_NAME_WITH_TYPE));

    MockInstanceDataManagerFactory fastFactory = new MockInstanceDataManagerFactory("fastServer");
    fastFactory.registerTable(
        new Schema.SchemaBuilder().addSingleValueDimension("col1", FieldSpec.DataType.STRING, "")
            .addSingleValueDimension("col2", FieldSpec.DataType.STRING, "")
            .addMetric("col3", FieldSpec.DataType.INT, 0)
            .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
            .setSchemaName(FAST_TABLE_NAME)
            .setEnableColumnBasedNullHandling(true)
            .build(),
        FAST_TABLE_WITH_TYPE);
    fastFactory.addSegment(FAST_TABLE_WITH_TYPE, QueryRunnerTest.buildRows(FAST_TABLE_WITH_TYPE));
    fastFactory.addSegment(FAST_TABLE_WITH_TYPE, QueryRunnerTest.buildRows(FAST_TABLE_WITH_TYPE));

    _reducerHostname = "localhost";
    _reducerPort = QueryTestUtils.getAvailablePort();
    Map<String, Object> reducerConfig = new HashMap<>();
    reducerConfig.put(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, _reducerHostname);
    reducerConfig.put(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, _reducerPort);
    _mailboxService = new MailboxService(_reducerHostname, _reducerPort, InstanceType.BROKER,
        new PinotConfiguration(reducerConfig));
    _mailboxService.start();

    _slowServer = new QueryServerEnclosure(slowFactory);
    _slowServer.start();
    _fastServer = new QueryServerEnclosure(fastFactory);
    _fastServer.start();

    QueryServerInstance slowInstance =
        new QueryServerInstance("Server_localhost_" + _slowServer.getPort(), "localhost", _slowServer.getPort(),
            _slowServer.getPort());
    QueryServerInstance fastInstance =
        new QueryServerInstance("Server_localhost_" + _fastServer.getPort(), "localhost", _fastServer.getPort(),
            _fastServer.getPort());
    _servers.put(slowInstance, _slowServer);
    _servers.put(fastInstance, _fastServer);

    Map<String, Schema> schemaMap = new HashMap<>();
    schemaMap.putAll(slowFactory.getRegisteredSchemaMap());
    schemaMap.putAll(fastFactory.getRegisteredSchemaMap());
    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(_reducerPort, _slowServer.getPort(),
        _fastServer.getPort(), schemaMap, slowFactory.buildTableSegmentNameMap(),
        fastFactory.buildTableSegmentNameMap(), null);
  }

  @AfterClass
  public void tearDownClass() {
    if (_mailboxService != null) {
      _mailboxService.shutdown();
    }
    if (_slowServer != null) {
      _slowServer.shutDown();
    }
    if (_fastServer != null) {
      _fastServer.shutDown();
    }
  }

  @Test
  public void testTimeoutOverflowBreakReturnsPartialResults() {
    String sql = queryPrefix("BREAK") + "SELECT col1 FROM " + TABLE_NAME + " LIMIT 5";
    QueryDispatcher.QueryResult result = queryRunner(sql, false);
    Assert.assertNull(result.getProcessingException(), "Expected partial success without exception");
    ResultTable resultTable = result.getResultTable();
    Assert.assertNotNull(resultTable, "Result table should be present even for partial results");
    Assert.assertTrue(hasTimeoutOverflowStats(result), "Leaf stats should record timeout overflow");
  }

  @Test
  public void testTimeoutOverflowThrowFailsQuery() {
    String sql = queryPrefix("THROW") + "SELECT col1 FROM " + TABLE_NAME + " LIMIT 5";
    QueryDispatcher.QueryResult result = queryRunner(sql, false);
    QueryProcessingException processingException = result.getProcessingException();
    Assert.assertNotNull(processingException, "Expected exception when overflow mode is THROW");
    Assert.assertEquals(processingException.getErrorCode(), QueryErrorCode.EXECUTION_TIMEOUT.getId());
  }

  @Test
  public void testTimeoutOverflowBreakOnJoin() {
    String sql = queryPrefix("BREAK")
        + "SELECT slow.col1, fast.col1 FROM " + TABLE_NAME + " slow JOIN "
        + FAST_TABLE_NAME + " fast ON slow.col2 = fast.col2";
    QueryDispatcher.QueryResult result = queryRunner(sql, false);
    Assert.assertNull(result.getProcessingException(), "Join should return partial results without error");
    Assert.assertNotNull(result.getResultTable());
    Assert.assertTrue(hasTimeoutOverflowStats(result));
  }

  @Test
  public void testTimeoutOverflowThrowOnJoin() {
    String sql = queryPrefix("THROW")
        + "SELECT slow.col1, fast.col1 FROM " + TABLE_NAME + " slow JOIN "
        + FAST_TABLE_NAME + " fast ON slow.col2 = fast.col2";
    QueryDispatcher.QueryResult result = queryRunner(sql, false);
    Assert.assertNotNull(result.getProcessingException(), "Join should fail when overflow mode THROW is used");
    Assert.assertEquals(result.getProcessingException().getErrorCode(), QueryErrorCode.EXECUTION_TIMEOUT.getId());
  }

  @Test
  public void testTimeoutOverflowBreakOnAggregation() {
    String sql = queryPrefix("BREAK")
        + "SELECT col2, COUNT(*) FROM " + TABLE_NAME + " GROUP BY col2";
    QueryDispatcher.QueryResult result = queryRunner(sql, false);
    Assert.assertNull(result.getProcessingException(), "Aggregation should emit partial results on BREAK");
    Assert.assertNotNull(result.getResultTable());
    Assert.assertTrue(hasTimeoutOverflowStats(result));
  }
  @Test
  public void testTimeoutOverflowBreakUsesDefaultLeafTimeout() {
    long timeoutMs = 100;
    long extraPassiveTimeoutMs = 50;
    String sql = queryPrefixWithoutLeafOverrides("BREAK", timeoutMs, extraPassiveTimeoutMs)
        + "SELECT col1 FROM " + TABLE_NAME + " LIMIT 5";
    QueryDispatcher.QueryResult result = queryRunner(sql, false);
    Assert.assertNull(result.getProcessingException(), "Expected partial results when relying on default leaf timeout");
    ResultTable resultTable = result.getResultTable();
    Assert.assertNotNull(resultTable);
    Assert.assertTrue(hasTimeoutOverflowStats(result));
  }

  private String queryPrefix(String overflowMode) {
    return buildQueryPrefix(overflowMode, QUERY_TIMEOUT_MS, QUERY_EXTRA_PASSIVE_TIMEOUT_MS, true);
  }

  private String queryPrefixWithoutLeafOverrides(String overflowMode, long timeoutMs, long extraPassiveTimeoutMs) {
    return buildQueryPrefix(overflowMode, timeoutMs, extraPassiveTimeoutMs, false);
  }

  private String buildQueryPrefix(String overflowMode, long timeoutMs, long extraPassiveTimeoutMs,
      boolean includeLeafOverrides) {
    StringBuilder builder = new StringBuilder();
    builder.append("SET useMultistageEngine='true';")
        .append("SET timeoutOverflowMode='").append(overflowMode).append("';")
        .append("SET timeoutMs='").append(timeoutMs).append("';")
        .append("SET extraPassiveTimeoutMs='").append(extraPassiveTimeoutMs).append("';");
    if (includeLeafOverrides) {
      builder.append("SET leafTimeoutMs='").append(LEAF_TIMEOUT_MS).append("';")
          .append("SET leafExtraPassiveTimeoutMs='").append(LEAF_EXTRA_PASSIVE_TIMEOUT_MS).append("';");
    }
    return builder.toString();
  }

  private boolean hasTimeoutOverflowStats(QueryDispatcher.QueryResult result) {
    AtomicBoolean overflow = new AtomicBoolean(false);
    for (MultiStageQueryStats.StageStats.Closed stageStats : result.getQueryStats()) {
      stageStats.forEach((type, statMap) -> {
        if (type == MultiStageOperator.Type.LEAF) {
          @SuppressWarnings("unchecked")
          org.apache.pinot.common.datatable.StatMap<LeafOperator.StatKey> stats =
              (org.apache.pinot.common.datatable.StatMap<LeafOperator.StatKey>) statMap;
          if (stats.getBoolean(LeafOperator.StatKey.TIMEOUT_OVERFLOW_REACHED)) {
            overflow.set(true);
          }
        }
      });
    }
    return overflow.get();
  }
}
