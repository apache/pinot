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
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.Nullable;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.core.instance.context.ServerContext;
import org.apache.pinot.integration.tests.custom.CustomDataQueryClusterIntegrationTest;
import org.apache.pinot.query.planner.plannode.JoinNode;
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
 * Exercises SQL planning and join execution on two real servers with Arrow disabled and enabled.
 * Single-threaded because the existing operator-factory override is process-wide.
 */
@Test(suiteName = "CustomClusterIntegrationTest", singleThreaded = true)
public final class ArrowClusterIntegrationTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "ArrowClusterIntegrationTest";
  private static final String ARROW_FLAG = CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_USE_ARROW;
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
    return SMALL_ROWS;
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
        .addSingleValueDimension("label", DataType.STRING)
        .addMetric("payload", DataType.LONG).build();
  }

  @Override
  public List<File> createAvroFiles()
      throws IOException {
    var avroSchema = SchemaBuilder.record("ArrowRows").fields()
        .requiredInt("kind").requiredInt("id").optionalInt("joinKey")
        .requiredString("label").optionalLong("payload").endRecord();
    try (var files = createAvroFilesAndWriters(avroSchema)) {
      var record = new GenericData.Record(avroSchema);
      // Duplicate keys on both sides, an unmatched key on each side, and NULL on both sides.
      Integer[] leftKeys = {7, 7, 8, 9, null};
      Integer[] rightKeys = {7, 7, 8, 10, null};
      for (int i = 0; i < leftKeys.length; i++) {
        append(files, record, 0, i + 1, leftKeys[i], "left-" + (i + 1), (long) (i + 1) * 10);
        append(files, record, 1, i + 11, rightKeys[i], "right-" + (i + 1),
            i == 2 ? null : (long) (i + 11) * 10);
      }
      return files.getAvroFiles();
    }
  }

  private static void append(AvroFilesAndWriters files, GenericData.Record record, int kind, int id,
      @Nullable Integer joinKey, String label, @Nullable Long payload)
      throws IOException {
    record.put("kind", kind);
    record.put("id", id);
    record.put("joinKey", joinKey);
    record.put("label", label);
    record.put("payload", payload);
    files.getWriters().get(id % files.getWriters().size()).append(record);
  }

  @Test(groups = "arrow-phase-2", timeOut = 300_000)
  public void testJoinsWithArrowDisabledAndEnabled()
      throws Exception {
    runWithArrowModes(this::assertSmallJoins);
  }

  private void runWithArrowModes(QueryAssertions assertions)
      throws Exception {
    setUseMultiStageQueryEngine(true);
    assertEquals(getSharedServerStarters().size(), 2);
    var serverContext = ServerContext.getInstance();
    Object previousProvider = serverContext.getQueryOperatorFactoryProvider();
    var observer = new ObservingFactoryProvider();
    var savedConfigs = componentConfigs().stream().map(config -> config.getProperty(ARROW_FLAG)).toList();
    try {
      for (boolean arrowEnabled : new boolean[]{false, true}) {
        for (var config : componentConfigs()) {
          config.setProperty(ARROW_FLAG, arrowEnabled);
        }
        restartComponents();
        // Server startup reinstalls its provider. Observe only after construction, before SQL runs.
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
          currentConfigs.get(i).setProperty(ARROW_FLAG, savedConfigs.get(i));
        }
        restartComponents();
      } finally {
        // The existing setter rejects null; restore the effective default when no override was installed.
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

  /** Observes the unmodified factory; the queue safely publishes selections made by both server threads. */
  private static final class ObservingFactoryProvider implements QueryOperatorFactoryProvider {
    private final Queue<JoinSelection> _selections = new ConcurrentLinkedQueue<>();
    private final JoinOperatorFactory _joins = new DefaultJoinOperatorFactory() {
      @Override
      public MultiStageOperator createJoinOperator(OpChainExecutionContext context, MultiStageOperator leftOperator,
          PlanNode leftPlanNode, MultiStageOperator rightOperator, PlanNode rightPlanNode, JoinNode joinNode) {
        var operator = super.createJoinOperator(context, leftOperator, leftPlanNode, rightOperator, rightPlanNode,
            joinNode);
        _selections.add(new JoinSelection(operator instanceof ArrowHashJoinOperator, joinNode.getJoinType()));
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

  private record JoinSelection(boolean _arrow, JoinRelType _joinType) {
  }
}
