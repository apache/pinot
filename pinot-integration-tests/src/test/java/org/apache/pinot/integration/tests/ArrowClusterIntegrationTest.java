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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.Nullable;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.metrics.MseMeter;
import org.apache.pinot.common.metrics.MseMetrics;
import org.apache.pinot.core.instance.context.ServerContext;
import org.apache.pinot.integration.tests.custom.CustomDataQueryClusterIntegrationTest;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.memory.ArrowBuffers;
import org.apache.pinot.query.runtime.operator.ArrowHashJoinOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.factory.AggregateOperatorFactory;
import org.apache.pinot.query.runtime.operator.factory.DefaultJoinOperatorFactory;
import org.apache.pinot.query.runtime.operator.factory.DefaultQueryOperatorFactoryProvider;
import org.apache.pinot.query.runtime.operator.factory.JoinOperatorFactory;
import org.apache.pinot.query.runtime.operator.factory.QueryOperatorFactoryProvider;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Exercises SQL planning, real servers and intermediate gRPC exchanges with Arrow disabled and enabled.
 * Uses the custom-data cluster's two servers; component restarts, not query options, change the Arrow flag.
 * The phase-2 SQL join test and phase-3 exchange test are independently selectable and have no ordering dependency.
 * The test is single-threaded because the existing operator-factory override is process-wide.
 */
@Test(suiteName = "CustomClusterIntegrationTest", singleThreaded = true)
public final class ArrowClusterIntegrationTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "ArrowClusterIntegrationTest";
  private static final String ARROW_FLAG = CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_USE_ARROW;
  private static final int FACT_ROWS = 65_536;
  private static final int FIRST_KEYS = 64;
  private static final int SECOND_KEYS = 63;
  private static final int SMALL_ROWS = 10;
  private static final long QUERY_TIMEOUT_MS = 30_000;
  private static final long CLEANUP_TIMEOUT_MS = 10_000;
  private static final String QUERY_OPTIONS = "SET timeoutMs=" + QUERY_TIMEOUT_MS + "; SET enableNullHandling=true; ";

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return SMALL_ROWS + FACT_ROWS + 2 * FIRST_KEYS + SECOND_KEYS;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    var tableConfig = super.createOfflineTableConfig();
    tableConfig.getIndexingConfig().setNullHandlingEnabled(true);
    return tableConfig;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).setEnableColumnBasedNullHandling(true)
        .addSingleValueDimension("kind", DataType.INT)
        .addSingleValueDimension("id", DataType.INT)
        .addSingleValueDimension("joinKey", DataType.INT)
        .addSingleValueDimension("routeKey", DataType.INT)
        .addSingleValueDimension("label", DataType.STRING)
        .addMetric("payload", DataType.LONG).build();
  }

  @Override
  public List<File> createAvroFiles()
      throws IOException {
    var avroSchema = SchemaBuilder.record("ArrowRows").fields()
        .requiredInt("kind").requiredInt("id").optionalInt("joinKey").requiredInt("routeKey")
        .requiredString("label").optionalLong("payload").endRecord();
    try (var files = createAvroFilesAndWriters(avroSchema)) {
      var record = new GenericData.Record(avroSchema);
      // Duplicate keys on both sides, an unmatched key on each side, and NULL on both sides.
      Integer[] leftKeys = {7, 7, 8, 9, null};
      Integer[] rightKeys = {7, 7, 8, 10, null};
      for (int i = 0; i < leftKeys.length; i++) {
        append(files, record, 0, i + 1, leftKeys[i], 0, "left-" + (i + 1), (long) (i + 1) * 10);
        append(files, record, 1, i + 11, rightKeys[i], 0, "right-" + (i + 1),
            i == 2 ? null : (long) (i + 11) * 10);
      }
      // Coprime distribution-key cardinalities require repartitioning the first join's output.
      for (int i = 0; i < FACT_ROWS; i++) {
        append(files, record, 2, i, i % FIRST_KEYS, i % SECOND_KEYS, "fact", (long) i);
      }
      for (int key = 0; key < FIRST_KEYS; key++) {
        append(files, record, 3, 2 * key, key, 0, "first", (long) key + 1);
        append(files, record, 3, 2 * key + 1, key, 0, "first-duplicate", (long) key + 1001);
      }
      for (int key = 0; key < SECOND_KEYS; key++) {
        append(files, record, 4, key, key, 0, "second", (long) key * 10);
      }
      return files.getAvroFiles();
    }
  }

  private static void append(AvroFilesAndWriters files, GenericData.Record record, int kind, int id,
      @Nullable Integer joinKey, int routeKey, String label, @Nullable Long payload)
      throws IOException {
    record.put("kind", kind);
    record.put("id", id);
    record.put("joinKey", joinKey);
    record.put("routeKey", routeKey);
    record.put("label", label);
    record.put("payload", payload);
    files.getWriters().get(id % files.getWriters().size()).append(record);
  }

  /**
   * Phase 2: SQL semantics, actual native join selection/execution and allocation cleanup.
   * This entry point does not assert IPC negotiation or require a native join-to-join mailbox boundary.
   */
  @Test(groups = "arrow-phase-2", timeOut = 300_000)
  public void testJoinsWithArrowDisabledAndEnabled()
      throws Exception {
    runWithArrowModes(this::assertSmallJoins, false);
  }

  /**
   * Phase 3: a separately selectable remote exchange between native join regions with observed IPC decoding.
   */
  @Test(groups = "arrow-phase-3", timeOut = 300_000)
  public void testIntermediateShuffleWithArrowDisabledAndEnabled()
      throws Exception {
    runWithArrowModes(this::assertIntermediateShuffle, true);
  }

  private void runWithArrowModes(QueryAssertions assertions, boolean enableBackpressure)
      throws Exception {
    setUseMultiStageQueryEngine(true);
    assertEquals(getSharedServerStarters().size(), 2, "The shuffle must have two physical servers");
    var serverContext = ServerContext.getInstance();
    Object previousProvider = serverContext.getQueryOperatorFactoryProvider();
    var observer = new ObservingFactoryProvider();
    var savedConfigs = componentConfigs().stream().map(ArrowClusterIntegrationTest::saveConfiguration).toList();
    try {
      for (boolean arrowEnabled : new boolean[]{false, true}) {
        configureComponents(componentConfigs(), arrowEnabled, enableBackpressure);
        restartComponents();
        // Server startup reinstalls its provider. Install the observer only after construction, before SQL runs.
        serverContext.setQueryOperatorFactoryProvider(observer);
        for (var buffers : serverArrowBuffers()) {
          assertEquals(buffers.isEnabled(), arrowEnabled, "Flag must be captured by the new mailbox service");
        }
        assertions.run(observer, arrowEnabled);
      }
    } finally {
      try {
        var currentConfigs = componentConfigs();
        for (int i = 0; i < savedConfigs.size(); i++) {
          savedConfigs.get(i).forEach(currentConfigs.get(i)::setProperty);
        }
        restartComponents();
      } finally {
        // The existing setter rejects null; the default provider restores the effective pre-test behavior.
        serverContext.setQueryOperatorFactoryProvider(
            previousProvider == null ? DefaultQueryOperatorFactoryProvider.INSTANCE : previousProvider);
      }
    }
  }

  private void assertSmallJoins(ObservingFactoryProvider observer, boolean arrowEnabled)
      throws Exception {
    String left = "(SELECT * FROM " + TABLE_NAME + " WHERE kind=0) l";
    String right = "(SELECT * FROM " + TABLE_NAME + " WHERE kind=1) r";
    String columns = "SELECT l.id, r.id, l.label, r.payload FROM ";
    String condition = " ON l.joinKey=r.joinKey ORDER BY l.id, r.id";
    String matched = "[1,11,\"left-1\",110],[1,12,\"left-1\",120],"
        + "[2,11,\"left-2\",110],[2,12,\"left-2\",120],[3,13,\"left-3\",null]";
    assertRows(observer, arrowEnabled, JoinRelType.INNER,
        columns + left + " INNER JOIN " + right + condition, "[" + matched + "]");
    assertRows(observer, arrowEnabled, JoinRelType.LEFT,
        columns + left + " LEFT JOIN " + right + condition,
        "[" + matched + ",[4,null,\"left-4\",null],[5,null,\"left-5\",null]]");
    String exists = "(SELECT 1 FROM " + TABLE_NAME + " r WHERE r.kind=1 AND r.joinKey=l.joinKey)";
    // A bounded probe prevents the SEMI join from becoming a dynamic filter inside a leaf scan.
    String boundedLeft = "(SELECT * FROM " + TABLE_NAME + " WHERE kind=0 ORDER BY id LIMIT 5) l";
    assertRows(observer, arrowEnabled, JoinRelType.SEMI,
        "SELECT l.id, l.label FROM " + boundedLeft + " WHERE EXISTS " + exists + " ORDER BY l.id",
        "[[1,\"left-1\"],[2,\"left-2\"],[3,\"left-3\"]]");
    // The planner lowers NOT EXISTS to LEFT + IS NULL; direct ANTI execution is covered by operator tests.
    assertRows(observer, arrowEnabled, JoinRelType.LEFT,
        "SELECT l.id, l.label FROM " + boundedLeft + " WHERE NOT EXISTS " + exists + " ORDER BY l.id",
        "[[4,\"left-4\"],[5,\"left-5\"]]");
  }

  private void assertRows(ObservingFactoryProvider observer, boolean arrowEnabled, JoinRelType joinType,
      String sql, String expectedRows)
      throws Exception {
    var response = executeObservedQuery(observer, arrowEnabled, joinType, sql);
    assertEquals(response.path("resultTable").path("rows").toString(), expectedRows, sql);
  }

  private JsonNode executeObservedQuery(ObservingFactoryProvider observer, boolean arrowEnabled, JoinRelType joinType,
      String sql)
      throws Exception {
    observer._selections.clear();
    var response = postQuery(QUERY_OPTIONS + sql);
    assertNoError(response);
    assertFalse(observer._selections.isEmpty(), "SQL must execute a join, not be optimized to a scan: " + sql);
    for (var selection : observer._selections) {
      assertEquals(selection._arrow, arrowEnabled, "Default factory chose the wrong implementation: " + sql);
      assertEquals(selection._joinType, joinType, "Planner did not produce the intended join: " + sql);
    }
    var joins = new ArrayList<JsonNode>();
    collectStats(response.path("stageStats"), "HASH_JOIN", joins);
    assertFalse(joins.isEmpty(), "A selected join must also appear in actual execution stats: " + response);
    assertTrue(joins.stream().mapToLong(node -> node.path("emittedRows").asLong()).sum() > 0,
        "The selected join implementations must execute and emit rows: " + response);
    assertArrowReleased();
    return response;
  }

  private void assertIntermediateShuffle(ObservingFactoryProvider observer, boolean arrowEnabled)
      throws Exception {
    // LEFT joins retain the first join as the second join's probe side. Do not add LIMIT or a DISTINCT/aggregate
    // between them: that would insert a heap boundary or let early termination truncate the exchange.
    // Retain both first-join keys in the result aggregates as well, so a key-pruning projection cannot introduce
    // a heap boundary between that join and its mailbox sender.
    String sql = "SELECT COUNT(*), SUM(f.id), SUM(d.payload), SUM(e.payload), "
        + "SUM(f.joinKey), SUM(d.joinKey), SUM(f.routeKey) FROM "
        + "(SELECT * FROM " + TABLE_NAME + " WHERE kind=2) f LEFT JOIN "
        + "(SELECT * FROM " + TABLE_NAME + " WHERE kind=3) d ON f.joinKey=d.joinKey LEFT JOIN "
        + "(SELECT * FROM " + TABLE_NAME + " WHERE kind=4) e ON f.routeKey=e.joinKey";
    var ipcMeter = MseMetrics.get().getMeteredValue(MseMeter.ARROW_IPC_MESSAGES_RECEIVED);
    long receivedBefore = ipcMeter.count();
    var response = executeObservedQuery(observer, arrowEnabled, JoinRelType.LEFT, sql);
    long ipcMessages = ipcMeter.count() - receivedBefore;
    if (arrowEnabled) {
      assertTrue(ipcMessages > 0, "The remote join exchange must decode actual Arrow IPC frames");
    } else {
      assertEquals(ipcMessages, 0L, "Disabled Arrow must not send IPC frames");
    }
    long secondPayload = 0;
    for (int i = 0; i < FACT_ROWS; i++) {
      secondPayload += 2L * (i % SECOND_KEYS) * 10;
    }
    long firstPayload = (long) (FACT_ROWS / FIRST_KEYS) * FIRST_KEYS * (1002 + FIRST_KEYS - 1);
    long firstKeySum = (long) FACT_ROWS * (FIRST_KEYS - 1);
    String expected = "[[" + (2L * FACT_ROWS) + "," + ((long) FACT_ROWS * (FACT_ROWS - 1)) + ","
        + firstPayload + "," + secondPayload + "," + firstKeySum + "," + firstKeySum + ","
        + (secondPayload / 10) + "]]";
    assertEquals(response.path("resultTable").path("rows").toString(), expected, sql);

    Set<Integer> joinStages = new HashSet<>();
    Set<Integer> serverPorts = new HashSet<>();
    for (var selection : observer._selections) {
      joinStages.add(selection._stage);
      serverPorts.add(selection._serverPort);
    }
    assertEquals(serverPorts.size(), 2, "Both physical servers must execute joins");
    assertEquals(joinStages.size(), 2, "Different distribution keys must create two intermediate join stages");
    Set<Integer> intermediateSenders = new HashSet<>();
    for (var selection : observer._selections) {
      for (int sender : selection._hashInputStages) {
        if (sender != selection._stage && joinStages.contains(sender)) {
          intermediateSenders.add(sender);
        }
      }
    }
    assertFalse(intermediateSenders.isEmpty(), "A join must consume a hash exchange from the other join stage");
    var sends = new ArrayList<JsonNode>();
    collectStats(response.path("stageStats"), "MAILBOX_SEND", sends);
    var intermediateSends = sends.stream()
        .filter(node -> intermediateSenders.contains(node.path("stage").asInt())).toList();
    assertFalse(intermediateSends.isEmpty(), "Missing executed join-to-join mailbox send");
    for (var send : intermediateSends) {
      assertEquals(send.path("children").size(), 1, "An intermediate sender must have one producer");
      assertEquals(send.path("children").get(0).path("type").asText(), "HASH_JOIN",
          "A heap projection/filter between the join and sender would not exercise native IPC");
      assertTrue(send.path("fanOut").asInt() >= 2, "Repartitioning must target both workers: " + send);
      assertTrue(send.path("rawMessages").asInt() > 0, "An in-process mailbox is not remote coverage: " + send);
      assertTrue(send.path("serializedBytes").asLong() > 0, "Remote exchange must serialize data: " + send);
      if (arrowEnabled) {
        // Send beyond the initial legacy frames; the decoded-IPC meter above verifies the negotiated codec.
        assertTrue(send.path("rawMessages").asInt() > 2 * (ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS + 1),
            "Expected multiple batches on the intermediate remote exchange: " + send);
      }
    }
  }

  private static void collectStats(JsonNode node, String type, List<JsonNode> matches) {
    if (type.equals(node.path("type").asText())) {
      matches.add(node);
    }
    for (var child : node.path("children")) {
      collectStats(child, type, matches);
    }
  }

  private List<ArrowBuffers> serverArrowBuffers() {
    return getSharedServerStarters().stream()
        .map(server -> server.getServerInstance().getWorkerQueryServer().getQueryRunner()
            .getMailboxService().getArrowBuffers()).toList();
  }

  private void assertArrowReleased() {
    for (var buffers : serverArrowBuffers()) {
      if (buffers.isEnabled()) {
        TestUtils.waitForCondition(ignored -> buffers.getAllocatedMemory() == 0, 20L, CLEANUP_TIMEOUT_MS,
            "Server Arrow allocations were not released after the query");
        assertEquals(buffers.getAllocatedMemory(), 0L);
      }
    }
  }

  private List<PinotConfiguration> componentConfigs() {
    var configs = new ArrayList<PinotConfiguration>();
    for (var server : getSharedServerStarters()) {
      configs.add(server.getConfig());
    }
    return configs;
  }

  private static Map<String, String> saveConfiguration(PinotConfiguration config) {
    var values = new HashMap<String, String>();
    values.put(ARROW_FLAG, config.getProperty(ARROW_FLAG));
    String backpressure = CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_SENDER_BACKPRESSURE_ENABLED;
    values.put(backpressure, config.getProperty(backpressure));
    return values;
  }

  private static void configureComponents(List<PinotConfiguration> configs, boolean arrowEnabled,
      boolean enableBackpressure) {
    for (var config : configs) {
      config.setProperty(ARROW_FLAG, arrowEnabled);
      if (enableBackpressure) {
        config.setProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_SENDER_BACKPRESSURE_ENABLED, true);
      }
    }
  }

  private void restartComponents()
      throws Exception {
    _sharedClusterTestSuite.restartServers();
    waitForAllDocsLoaded(60_000);
  }

  @FunctionalInterface
  private interface QueryAssertions {
    void run(ObservingFactoryProvider observer, boolean arrowEnabled)
        throws Exception;
  }

  /**
   * Observes the actual default factory without wrapping operators or changing native-consumer capability.
   * Both servers run in this JVM; the queue safely publishes immutable selection facts to the test thread.
   */
  private static final class ObservingFactoryProvider implements QueryOperatorFactoryProvider {
    private final Queue<JoinSelection> _selections = new ConcurrentLinkedQueue<>();
    private final JoinOperatorFactory _joins = new DefaultJoinOperatorFactory() {
      @Override
      public MultiStageOperator createJoinOperator(OpChainExecutionContext context, MultiStageOperator leftOperator,
          PlanNode leftPlanNode, MultiStageOperator rightOperator, PlanNode rightPlanNode, JoinNode joinNode) {
        var operator = super.createJoinOperator(context, leftOperator, leftPlanNode, rightOperator, rightPlanNode,
            joinNode);
        Set<Integer> inputStages = new HashSet<>();
        for (var input : List.of(leftPlanNode, rightPlanNode)) {
          if (input instanceof MailboxReceiveNode receive
              && receive.getDistributionType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            inputStages.add(receive.getSenderStageId());
          }
        }
        _selections.add(new JoinSelection(operator instanceof ArrowHashJoinOperator, joinNode.getJoinType(),
            context.getStageId(), context.getMailboxService().getPort(), Set.copyOf(inputStages)));
        return operator;
      }
    };

    @Override
    public JoinOperatorFactory getJoinOperatorFactory() {
      return _joins;
    }

    @Override
    public AggregateOperatorFactory getAggregateOperatorFactory() {
      return DefaultQueryOperatorFactoryProvider.INSTANCE.getAggregateOperatorFactory();
    }
  }

  private record JoinSelection(boolean _arrow, JoinRelType _joinType, int _stage, int _serverPort,
                               Set<Integer> _hashInputStages) {
  }
}
