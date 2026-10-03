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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.runtime.MultiStageStatsTreeBuilder;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.rewriter.RlsUtils;
import org.assertj.core.api.Assertions;
import org.intellij.lang.annotations.Language;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;


/// all special tests that doesn't fit into [org.apache.pinot.query.runtime.queries.ResourceBasedQueriesTest]
/// pattern goes here.
public class QueryRunnerTest extends QueryRunnerTestBase {
  //@formatter:off
  public static final Object[][] ROWS = new Object[][]{
      new Object[]{"foo", "foo", 1},
      new Object[]{"bar", "bar", 42},
      new Object[]{"alice", "alice", 1},
      new Object[]{"bob", "foo", 42},
      new Object[]{"charlie", "bar", 1}
  };
  //@formatter:on
  public static final Schema.SchemaBuilder SCHEMA_BUILDER;

  static {
    SCHEMA_BUILDER = new Schema.SchemaBuilder().addSingleValueDimension("col1", FieldSpec.DataType.STRING, "")
        .addSingleValueDimension("col2", FieldSpec.DataType.STRING, "")
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .addMetric("col3", FieldSpec.DataType.INT, 0)
        .setSchemaName("defaultSchemaName")
        .setEnableColumnBasedNullHandling(true);
  }

  public static List<GenericRow> buildRows(String tableName) {
    List<GenericRow> rows = new ArrayList<>(ROWS.length);
    for (int i = 0; i < ROWS.length; i++) {
      GenericRow row = new GenericRow();
      row.putValue("col1", ROWS[i][0]);
      row.putValue("col2", ROWS[i][1]);
      row.putValue("col3", ROWS[i][2]);
      row.putValue("ts",
          TableType.OFFLINE.equals(TableNameBuilder.getTableTypeFromTableName(tableName)) ? System.currentTimeMillis()
              - TimeUnit.DAYS.toMillis(2) : System.currentTimeMillis());
      rows.add(row);
    }
    return rows;
  }

  protected Map<String, Object> getConfiguration() {
    return Map.of();
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    MockInstanceDataManagerFactory factory1 = new MockInstanceDataManagerFactory("server1");
    factory1.registerTable(SCHEMA_BUILDER.setSchemaName("a").build(), "a_REALTIME");
    factory1.registerTable(SCHEMA_BUILDER.setSchemaName("b").build(), "b_REALTIME");
    factory1.registerTable(SCHEMA_BUILDER.setSchemaName("c").build(), "c_OFFLINE");
    factory1.registerTable(SCHEMA_BUILDER.setSchemaName("d").build(), "d");
    factory1.registerTable(SCHEMA_BUILDER.setSchemaName("tbl-escape-naming").build(), "tbl-escape-naming_OFFLINE");
    factory1.addSegment("a_REALTIME", buildRows("a_REALTIME"));
    factory1.addSegment("a_REALTIME", buildRows("a_REALTIME"));
    factory1.addSegment("b_REALTIME", buildRows("b_REALTIME"));
    factory1.addSegment("c_OFFLINE", buildRows("c_OFFLINE"));
    factory1.addSegment("d_OFFLINE", buildRows("d_OFFLINE"));
    factory1.addSegment("tbl-escape-naming_OFFLINE", buildRows("tbl-escape-naming_OFFLINE"));

    MockInstanceDataManagerFactory factory2 = new MockInstanceDataManagerFactory("server2");
    factory2.registerTable(SCHEMA_BUILDER.setSchemaName("a").build(), "a_REALTIME");
    factory2.registerTable(SCHEMA_BUILDER.setSchemaName("c").build(), "c_OFFLINE");
    factory2.registerTable(SCHEMA_BUILDER.setSchemaName("d").build(), "d");
    factory2.addSegment("a_REALTIME", buildRows("a_REALTIME"));
    factory2.addSegment("c_OFFLINE", buildRows("c_OFFLINE"));
    factory2.addSegment("c_OFFLINE", buildRows("c_OFFLINE"));
    factory2.addSegment("d_OFFLINE", buildRows("d_OFFLINE"));
    factory2.addSegment("d_REALTIME", buildRows("d_REALTIME"));

    // Setting up H2 for validation
    setH2Connection();
    Schema schema = SCHEMA_BUILDER.build();
    for (String tableName : Arrays.asList("a", "b", "c", "d")) {
      addTableToH2(tableName, schema);
      addDataToH2(tableName, schema, factory1.buildTableRowsMap().get(tableName));
      addDataToH2(tableName, schema, factory2.buildTableRowsMap().get(tableName));
    }

    _reducerHostname = "localhost";
    _reducerPort = QueryTestUtils.getAvailablePort();
    Map<String, Object> reducerConfig = new HashMap<>();
    reducerConfig.put(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, _reducerHostname);
    reducerConfig.put(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, _reducerPort);
    _mailboxService =
        new MailboxService(_reducerHostname, _reducerPort, InstanceType.BROKER, new PinotConfiguration(reducerConfig));
    _mailboxService.start();

    QueryServerEnclosure server1 = new QueryServerEnclosure(factory1, getConfiguration());
    server1.start();
    // Start server1 to ensure the next server will have a different port.
    QueryServerEnclosure server2 = new QueryServerEnclosure(factory2, getConfiguration());
    server2.start();
    // this doesn't test the QueryServer functionality so the server port can be the same as the mailbox port.
    // this is only use for test identifier purpose.
    int port1 = server1.getPort();
    int port2 = server2.getPort();
    _servers.put(new QueryServerInstance("Server_localhost_" + port1, "localhost", port1, port1), server1);
    _servers.put(new QueryServerInstance("Server_localhost_" + port2, "localhost", port2, port2), server2);

    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(_reducerPort, server1.getPort(), server2.getPort(),
        factory1.getRegisteredSchemaMap(), factory1.buildTableSegmentNameMap(), factory2.buildTableSegmentNameMap(),
        null);
  }

  @AfterClass
  public void tearDown() {
    for (QueryServerEnclosure server : _servers.values()) {
      server.shutDown();
    }
    _mailboxService.shutdown();
  }

  /// The self stats of a node are the node's own value minus its children's. A mailbox send reports its stats from
  /// inside the getNextBlock() call whose time it is still spending, so unless that call is accounted first it
  /// reports less than the input whose call it contains and the subtraction goes negative. A query whose filter
  /// matches nothing makes the end-of-stream block the only block a stage handles, which is when the whole of the
  /// send's time would be missing.
  @Test
  public void testSelfStatsAreNotNegative() {
    @Language("sql")
    String sql = "SELECT col1, COUNT(*) FROM a WHERE col1 = 'no-such-value' GROUP BY col1";
    QueryDispatcher.QueryResult queryResult = queryRunner(sql, true);
    Map<Integer, DispatchablePlanFragment> planNodes = planQuery(sql).getQueryPlan().getQueryStageMap();
    ObjectNode statsTree =
        new MultiStageStatsTreeBuilder(planNodes, queryResult.getQueryStats()).jsonStatsByStage(1);

    int checked = assertSelfStatsAreNotNegative(statsTree);
    Assert.assertTrue(checked > 0, "expected some self stats to check, got: " + statsTree);
  }

  /// Asserts that no self stat in the tree is negative, and returns how many were checked.
  private static int assertSelfStatsAreNotNegative(JsonNode node) {
    int checked = 0;
    for (String statName : List.of("selfExecutionTimeMs", "selfClockTimeMs", "selfAllocatedMB", "selfGcTimeMs")) {
      JsonNode stat = node.get(statName);
      if (stat != null) {
        Assert.assertTrue(stat.asLong() >= 0,
            statName + " is " + stat.asLong() + ", which means this node reported less than its children, for node "
                + node);
        checked++;
      }
    }
    for (JsonNode child : node.path("children")) {
      checked += assertSelfStatsAreNotNegative(child);
    }
    return checked;
  }

  /// Runs a shuffling query over the two-server setup and checks the per-worker stats reported by every stage.
  /// This is the only place these stats are exercised end to end, through real multi-worker stages and the
  /// cross-server merge of their stat maps.
  @Test
  public void testPerWorkerStats() {
    ObjectNode statsTree = statsTreeOf("SELECT col1, COUNT(*) FROM a GROUP BY col1");

    // Idle workers are reported rather than active ones, so a query where every worker did something must not
    // report any: their absence is the healthy signal.
    Assert.assertNull(findFieldOwner(statsTree, "nonActiveWorkers"),
        "expected no idle worker in a query where every worker contributes, got: " + statsTree);

    // Assertions comparing a single worker's stats against the stage totals collapse to identities on a
    // single-worker stage, so require a stage that actually ran on several workers, otherwise this test would keep
    // passing if the cross-worker merge broke.
    int multiWorkerSends = assertSendStats(statsTree);
    Assert.assertTrue(multiWorkerSends > 0,
        "expected a multi-worker send reporting maxEmittedRows and maxClockTimeMs, got: " + statsTree);
  }

  /// Each operator decides for itself which workers it was idle on, so a query whose filter matches nothing
  /// separates them: the leaf operators were handed segments and are not idle, while everything above them sent
  /// and received nothing and is idle on every worker.
  @Test
  public void testPerWorkerStatsWhenNothingMatches() {
    ObjectNode statsTree = statsTreeOf("SELECT col1, COUNT(*) FROM a WHERE col1 = 'no-such-value' GROUP BY col1");

    // StatMap drops zero-valued keys, so an absent field means zero throughout.
    ObjectNode leaf = findNodeOfType(statsTree, "LEAF");
    Assert.assertNotNull(leaf, "expected a LEAF node in " + statsTree);
    Assert.assertNull(leaf.get("nonActiveWorkers"),
        "expected workers with segments assigned not to be idle even though they emitted nothing: " + leaf);

    ObjectNode leafStageSend = findSendAboveLeaf(statsTree);
    Assert.assertNotNull(leafStageSend, "expected a leaf stage in " + statsTree);
    Assert.assertEquals(leafStageSend.path("emittedRows").asLong(0), 0L, "expected the leaf stage to send nothing");
    Assert.assertEquals(leafStageSend.path("nonActiveWorkers").asLong(0),
        leafStageSend.path("parallelism").asLong(0),
        "expected every worker of a send that sent nothing to be idle: " + leafStageSend);

    ObjectNode receive = findNodeOfType(statsTree, "MAILBOX_RECEIVE");
    Assert.assertNotNull(receive, "expected a MAILBOX_RECEIVE node in " + statsTree);
    Assert.assertEquals(receive.path("nonActiveWorkers").asLong(0), receive.path("parallelism").asLong(0),
        "expected every worker of a receive that got no row to be idle: " + receive);
  }

  private ObjectNode statsTreeOf(@Language("sql") String sql) {
    QueryDispatcher.QueryResult queryResult = queryRunner(sql, true);
    Map<Integer, DispatchablePlanFragment> planNodes = planQuery(sql).getQueryPlan().getQueryStageMap();
    return new MultiStageStatsTreeBuilder(planNodes, queryResult.getQueryStats()).jsonStatsByStage(1);
  }

  @Nullable
  private static ObjectNode findNodeOfType(JsonNode node, String type) {
    if (type.equals(node.path("type").asText())) {
      return (ObjectNode) node;
    }
    for (JsonNode child : node.path("children")) {
      ObjectNode found = findNodeOfType(child, type);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /// Returns the first node in the tree carrying `field`, or null if none does.
  @Nullable
  private static ObjectNode findFieldOwner(JsonNode node, String field) {
    if (node.get(field) != null) {
      return (ObjectNode) node;
    }
    for (JsonNode child : node.path("children")) {
      ObjectNode found = findFieldOwner(child, field);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /// Returns the MAILBOX_SEND node of the leaf stage, that is, the one holding the LEAF operator.
  @Nullable
  private static ObjectNode findSendAboveLeaf(JsonNode node) {
    for (JsonNode child : node.path("children")) {
      if ("MAILBOX_SEND".equals(node.path("type").asText()) && "LEAF".equals(child.path("type").asText())) {
        return (ObjectNode) node;
      }
      ObjectNode found = findSendAboveLeaf(child);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /// Asserts the per-worker invariants on every send node reporting them, and returns how many of those ran on
  /// more than one worker.
  private static int assertSendStats(JsonNode node) {
    int multiWorker = 0;
    JsonNode maxEmittedRows = node.get("maxEmittedRows");
    if (maxEmittedRows != null) {
      long max = maxEmittedRows.asLong();
      long emitted = node.path("emittedRows").asLong(0);
      long parallelism = node.path("parallelism").asLong(0);
      String ctx = " for node " + node;

      // Both are counts over one worker while emittedRows is the sum over all of them, so neither can exceed it.
      // This is what catches a merge function summing where it should take an extremum.
      Assert.assertTrue(max <= emitted, "maxEmittedRows " + max + " exceeds emittedRows " + emitted + ctx);

      long maxClockTimeMs = node.path("maxClockTimeMs").asLong(0);
      long executionTimeMs = node.path("executionTimeMs").asLong(0);
      Assert.assertTrue(maxClockTimeMs <= executionTimeMs,
          "maxClockTimeMs " + maxClockTimeMs + " exceeds the summed executionTimeMs " + executionTimeMs + ctx);
      Assert.assertTrue(maxClockTimeMs >= node.path("clockTimeMs").asLong(0),
          "maxClockTimeMs " + maxClockTimeMs + " is below the average clockTimeMs" + ctx);

      if (parallelism > 1) {
        multiWorker++;
      }
    }
    for (JsonNode child : node.path("children")) {
      multiWorker += assertSendStats(child);
    }
    return multiWorker;
  }

  /// Test compares with expected row count only.
  @Test(dataProvider = "testDataWithSqlToFinalRowCount")
  public void testSqlWithFinalRowCountChecker(String sql, int expectedRows) {
    ResultTable resultTable = queryRunner(sql, false).getResultTable();
    Assert.assertEquals(resultTable.getRows().size(), expectedRows);
  }

  /// Test automatically compares against H2.
  ///
  /// @deprecated do not add to this test set. this class will be broken down and clean up.
  ///   add your test to the appropriate files in [org.apache.pinot.query.runtime.queries] instead.
  @Test(dataProvider = "testSql")
  public void testSqlWithH2Checker(String sql)
      throws Exception {
    ResultTable resultTable = queryRunner(sql, false).getResultTable();
    // query H2 for data
    List<Object[]> expectedRows = queryH2(sql);
    compareRowEquals(resultTable, expectedRows);
  }

  /// Test compares against its desired exceptions.
  @Test(dataProvider = "testDataWithSqlExecutionExceptions")
  public void testSqlWithExceptionMsgChecker(String sql, @Language("regexp") String expectedError) {
    try {
      // query pinot
      QueryDispatcher.QueryResult queryResult = queryRunner(sql, false);
      if (queryResult.getProcessingException() != null) {
        throw new RuntimeException(queryResult.getProcessingException().getMessage());
      }
      ResultTable resultTable = queryResult.getResultTable();
      Assert.fail("Expected error with message '" + expectedError + "'. But instead rows were returned: "
          + JsonUtils.objectToPrettyString(resultTable));
    } catch (Exception e) {
      String exceptionMessage = e.getMessage();
      Assertions.assertThat(exceptionMessage)
          .withFailMessage("Exception should contain: " + expectedError + ", but found: " + exceptionMessage)
          .contains(expectedError);
    }
  }

  /// RLS filters are stamped onto the leaf's query options, which is also the only place the planner records that the
  /// leaf must finalize its aggregates (`is_partitioned_by_group_by_keys` makes the aggregate `AggType.DIRECT`, so no
  /// stage above it can finalize anything). Stamping must merge, not replace: dropping the flag makes the leaf emit a
  /// raw `IntOpenHashSet` into a column typed `INT`. Table b lives on a single server, so one row per group.
  @Test
  public void testDirectAggregateWithRowLevelSecurityFilter() {
    String sql = "SELECT /*+ aggOptions(is_partitioned_by_group_by_keys='true') */ col1, DISTINCTCOUNT(col3) FROM b "
        + "GROUP BY col1 ORDER BY col1";
    Map<String, String> rlsFilters = Map.of(RlsUtils.buildRlsFilterKey("b"), "col3 > 1");
    QueryDispatcher.QueryResult queryResult = queryRunner(sql, false, rlsFilters);
    Assert.assertNull(queryResult.getProcessingException(), "Query failed: " + queryResult.getProcessingException());
    // The RLS filter keeps only the two rows with col3 = 42.
    compareRowEquals(queryResult.getResultTable(), List.of(new Object[]{"bar", 1}, new Object[]{"bob", 1}), true);
  }

  @DataProvider(name = "emptyDynamicBroadcastAggregations")
  public Object[][] emptyDynamicBroadcastAggregations() {
    return new Object[][]{
        {
            "SELECT /*+ joinOptions(join_strategy='dynamic_broadcast') */ COUNT(*) FROM a "
                + "WHERE a.col1 IN (SELECT b.col2 FROM b WHERE b.col3 < 0)",
            List.<Object[]>of(new Object[]{0L})
        },
        {
            "SELECT /*+ joinOptions(join_strategy='dynamic_broadcast') */ COUNT(a.col3) FROM a "
                + "WHERE a.col1 IN (SELECT b.col2 FROM b WHERE b.col3 < 0)",
            List.<Object[]>of(new Object[]{0L})
        }
    };
  }

  @Test(dataProvider = "emptyDynamicBroadcastAggregations")
  public void testEmptyDynamicBroadcastAggregation(String sql, List<Object[]> expectedRows) {
    QueryDispatcher.QueryResult queryResult = queryRunner(sql, false);
    assertNull(queryResult.getProcessingException(), "Query failed: " + queryResult.getProcessingException());
    compareRowEquals(queryResult.getResultTable(), expectedRows, true);
  }

  @DataProvider(name = "testDataWithSqlToFinalRowCount")
  protected Object[][] provideTestSqlAndRowCount() {
    //@formatter:off
    return new Object[][]{
        // special hint test, the table is not actually partitioned by col1, thus this hint gives wrong result. but
        // b/c in order to test whether this hint produces the proper optimized plan, we are making this assumption
        new Object[]{
            "SELECT /*+ aggOptions(is_partitioned_by_group_by_keys='true') */ col1, COUNT(*) FROM a GROUP BY 1 "
                + "ORDER BY 2",
            10
        },

        // special hint test, we want to try if dynamic broadcast works for just any random table */
        new Object[]{
            "SELECT /*+ joinOptions(join_strategy='dynamic_broadcast') */ col1 FROM a WHERE a.col1 IN "
                + "(SELECT b.col2 FROM b WHERE b.col3 < 10) AND a.col3 > 0",
            9
        },

        // using join clause
        new Object[]{"SELECT * FROM a JOIN b USING (col1)", 15},

        // cannot compare with H2 w/o an ORDER BY because ordering is indeterminate
        new Object[]{"SELECT * FROM a LIMIT 2", 2},

        // test dateTrunc
        //   - on leaf stage
        new Object[]{"SELECT dateTrunc('DAY', ts) FROM a LIMIT 10", 10},
        new Object[]{"SELECT dateTrunc('DAY', CAST(col3 AS BIGINT)) FROM a LIMIT 10", 10},
        //   - on intermediate stage
        new Object[]{"SELECT dateTrunc('DAY', a.ts + b.ts) FROM a JOIN b ON a.col1 = b.col1 AND a.col2 = b.col2", 15},
        new Object[]{"SELECT dateTrunc('DAY', CAST(MAX(a.col3) AS BIGINT)) FROM a", 1},

        // ScalarFunction
        // test function can be used in predicate/leaf/intermediate stage (using regexpLike)
        new Object[]{"SELECT a.col1, b.col1 FROM a JOIN b ON a.col3 = b.col3 WHERE regexpLikeVar(a.col2, b.col1)", 9},
        new Object[]{"SELECT a.col1, b.col1 FROM a JOIN b ON a.col3 = b.col3 WHERE regexp_like_var(a.col2, b.col1)", 9},
        new Object[]{"SELECT regexpLikeVar(a.col1, b.col1) FROM a JOIN b ON a.col3 = b.col3", 39},
        new Object[]{"SELECT regexp_like_var(a.col1, b.col1) FROM a JOIN b ON a.col3 = b.col3", 39},

        // test function with @ScalarFunction annotation and alias works (using round_decimal)
        new Object[]{"SELECT roundDecimal(col3) FROM a", 15},
        new Object[]{"SELECT round_decimal(col3) FROM a", 15},
        new Object[]{"SELECT col1, roundDecimal(COUNT(*)) FROM a GROUP BY col1", 5},
        new Object[]{"SELECT col1, round_decimal(COUNT(*)) FROM a GROUP BY col1", 5},

        // test queries with special query options attached
        //   - when leaf limit is set, each server returns multiStageLeafLimit number of rows only.
        new Object[]{"SET multiStageLeafLimit = 1; SELECT * FROM a", 2},

        // test groups limit in both leaf and intermediate stage
        new Object[]{"SET numGroupsLimit = 1; SELECT col1, COUNT(*) FROM a GROUP BY col1", 1},
        new Object[]{"SET numGroupsLimit = 2; SELECT col1, COUNT(*) FROM a GROUP BY col1", 2},
        new Object[]{
            "SET numGroupsLimit = 1; "
                + "SELECT a.col2, b.col2, COUNT(*) FROM a JOIN b USING (col1) GROUP BY a.col2, b.col2",
            1
        },
        new Object[]{
            "SET numGroupsLimit = 2; "
                + "SELECT a.col2, b.col2, COUNT(*) FROM a JOIN b USING (col1) GROUP BY a.col2, b.col2",
            2
        },
        // TODO: Consider pushing down hint to the leaf stage
        new Object[]{
            "SET numGroupsLimit = 2; "
                + "SELECT /*+ aggOptions(num_groups_limit='1') */ col1, COUNT(*) FROM a GROUP BY col1",
            2
        },
        new Object[]{
            "SET numGroupsLimit = 2; "
                + "SELECT /*+ aggOptions(num_groups_limit='1') */ a.col2, b.col2, COUNT(*) FROM a JOIN b USING (col1) "
                + "GROUP BY a.col2, b.col2",
            1
        },
        new Object[]{"SELECT * FROM default.\"tbl-escape-naming\"", 5},
        new Object[]{"SELECT * FROM \"default\".\"tbl-escape-naming\"", 5}
    };
    //@formatter:on
  }

  @DataProvider(name = "testDataWithSqlExecutionExceptions")
  protected Iterator<Object[]> provideTestSqlWithExecutionException() {
    List<Object[]> testCases = new ArrayList<>();
    // Missing index
    testCases.add(new Object[]{"SELECT col1 FROM a WHERE textMatch(col1, 'f') LIMIT 10", "without text index"});
    testCases.add(new Object[]{"SELECT col1, textMatch(col1, 'f') FROM a LIMIT 10", "without text index"});

    // Query hint with dynamic broadcast pipeline breaker should return error upstream
    testCases.add(new Object[]{
        "SELECT /*+ joinOptions(join_strategy='dynamic_broadcast') */ col1 FROM a WHERE a.col1 IN "
            + "(SELECT b.col2 FROM b WHERE textMatch(col1, 'f')) AND a.col3 > 0",
        "without text index"
    });

    // Timeout exception should occur with this option:
    // - During submission: "Error occurred during stage submission: Timeout"
    // - During execution on receiver side: "Timed out on stage 0 waiting for data from child stage 1"
    // - During execution on sender side: "Received 1 error from stage 1 on serverId: Timing out on: HASH_JOIN"
    testCases.add(new Object[]{
        "SET timeoutMs = 1; SELECT * FROM a JOIN b ON a.col1 = b.col1 JOIN c ON a.col1 = c.col1",
        "Tim"
    });

    // Function with incorrect argument signature should throw runtime exception when casting string to numeric
    testCases.add(new Object[]{"SELECT least(a.col2, b.col3) FROM a JOIN b ON a.col1 = b.col1", "For input string:"});

    // Scalar function that doesn't have a valid use should throw an exception on the leaf stage
    //   - predicate only functions:
    testCases.add(new Object[]{"SELECT * FROM a WHERE textMatch(col1, 'f')", "without text index"});
    testCases.add(new Object[]{"SELECT * FROM a WHERE text_match(col1, 'f')", "without text index"});

    //  - transform only functions
    testCases.add(new Object[]{"SELECT jsonExtractKey(col1, 'path') FROM a", "was expecting (JSON String"});
    testCases.add(new Object[]{"SELECT json_extract_key(col1, 'path') FROM a", "was expecting (JSON String"});

    //  - PlaceholderScalarFunction registered will throw on intermediate stage, but works on leaf stage.
    //    - checked "Illegal Json Path" as col1 is not actually a json string, but the call is correctly triggered.
    testCases.add(
        new Object[]{"SELECT CAST(jsonExtractScalar(col1, 'path', 'INT') AS INT) FROM a", "Cannot resolve JSON path"});
    //    - a constant-foldable jsonPath must be folded by PinotEvaluateLiteralRule and then applied like a literal.
    //      Reaching "Cannot resolve JSON path" (rather than ParserUtils' "single-quoted literal values") is what
    //      proves the fold happened, so this pins the reason jsonPath does not require a literal in
    //      TransformFunctionType#jsonExtractScalarOperandTypeChecker.
    testCases.add(new Object[]{
        "SELECT CAST(jsonExtractScalar(col1, CONCAT('pa', 'th'), 'INT') AS INT) FROM a", "Cannot resolve JSON path"});
    //    - the flip side: a jsonPath that cannot fold to a literal is still rejected, on the leaf stage rather than
    //      during validation. Covers all four variants, which share the operand type checker.
    for (String jsonExtractScalar : new String[]{
        "jsonExtractScalar", "jsonExtractScalarFast", "jsonExtractScalarFirstMatch", "jsonExtractScalarFory"
    }) {
      testCases.add(new Object[]{
          "SELECT " + jsonExtractScalar + "(col1, col2, 'INT') FROM a",
          "Expect the 2nd and 3rd arguments of transform function: " + jsonExtractScalar
              + "(jsonFieldName, 'jsonPath', 'resultsType', ['defaultValue']) to be single-quoted literal values"
      });
    }
    //    - checked function cannot be found b/c there's no intermediate stage impl for json_extract_scalar
    testCases.add(new Object[]{
        "SELECT CAST(json_extract_scalar(a.col1, b.col2, 'INT') AS INT) FROM a JOIN b ON a.col1 = b.col1",
        "Unsupported function: JSONEXTRACTSCALAR"
    });

    // Positive int keys (only included ones that will be parsed for this query)
    for (String key : new String[]{
        QueryOptionKey.MAX_EXECUTION_THREADS,
        QueryOptionKey.NUM_GROUPS_LIMIT,
        QueryOptionKey.MAX_INITIAL_RESULT_HOLDER_CAPACITY,
        QueryOptionKey.MAX_STREAMING_PENDING_BLOCKS,
        QueryOptionKey.MAX_ROWS_IN_JOIN
    }) {
      for (String value : new String[]{"-10000000000", "-2147483648", "-1", "0", "2147483648", "10000000000"}) {
        testCases.add(new Object[]{
            "set " + key + " = " + value + "; SELECT col1, count(*) FROM a GROUP BY col1",
            key + " must be a number between 1 and 2^31-1, got: " + value
        });
      }
    }

    return testCases.iterator();
  }
}
