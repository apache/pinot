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
package org.apache.pinot.broker.broker;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.broker.stats.BrokerTableStatsManager;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Starts a real broker with `pinot.broker.stats.enabled=true` against a real controller and
/// ZooKeeper, and asserts that statistics actually arrive in the store.
///
/// Everything else in this feature is unit-tested against hand-built ZK records, which cannot see
/// the wiring: the listener provider being registered before the routing manager initialises, the
/// `pinot.broker.stats.*` subset reaching the provider, the directory default, and whether
/// sqlite-jdbc can even load its native library in an assembled broker. Each of those fails by
/// silently collecting nothing -- statistics are optional, so no existing assertion would notice.
///
/// Justifies its own cluster because it needs a non-default component configuration: the feature is
/// off by default, so it cannot ride an existing broker fixture.
public class BrokerStatsCollectionIntegrationTest extends ControllerTest {

  private static final String RAW_TABLE_NAME = "statsTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final String TIME_COLUMN_NAME = "daysSinceEpoch";
  private static final int BROKER_QUERY_PORT = 18103;
  private static final int NUM_SERVERS = 1;
  private static final int NUM_OFFLINE_SEGMENTS = 5;
  /// SegmentMetadataMockUtils gives every mocked segment this many documents.
  private static final int DOCS_PER_SEGMENT = 100;

  private HelixBrokerStarter _brokerStarter;
  private Path _statsDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();

    _statsDir = Files.createTempDirectory("broker-stats-it-");

    Map<String, Object> properties = new HashMap<>();
    properties.put(Helix.KEY_OF_BROKER_QUERY_PORT, BROKER_QUERY_PORT);
    properties.put(Helix.CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    properties.put(Helix.CONFIG_OF_ZOOKEEPER_SERVER, getZkUrl());
    properties.put(Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, 0);
    // The configuration under test. The store name is left unset on purpose, so the default
    // (sqlite) is what actually gets exercised.
    properties.put(Broker.CONFIG_OF_STATS_ENABLED, true);
    properties.put(Broker.CONFIG_OF_STATS_DIR, _statsDir.toString());

    _brokerStarter = new HelixBrokerStarter();
    _brokerStarter.init(new PinotConfiguration(properties));
    _brokerStarter.start();

    addFakeServerInstancesToAutoJoinHelixCluster(NUM_SERVERS, true);

    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addDateTime(TIME_COLUMN_NAME, FieldSpec.DataType.INT, "EPOCH|DAYS", "1:DAYS").build();
    _helixResourceManager.addSchema(schema, true, false);
    TableConfig offlineTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .setTimeType(TimeUnit.DAYS.name()).build();
    _helixResourceManager.addTable(offlineTableConfig);
    TableConfig realtimeTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .setTimeType(TimeUnit.DAYS.name()).setStreamConfigs(getStreamConfigs()).setNumReplicas(1).build();
    _helixResourceManager.addTable(realtimeTableConfig);

    for (int i = 0; i < NUM_OFFLINE_SEGMENTS; i++) {
      _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(RAW_TABLE_NAME), "downloadUrl");
    }

    TestUtils.waitForCondition(aVoid -> {
      ExternalView offlineView = _helixAdmin.getResourceExternalView(getHelixClusterName(), OFFLINE_TABLE_NAME);
      ExternalView realtimeView = _helixAdmin.getResourceExternalView(getHelixClusterName(), REALTIME_TABLE_NAME);
      return offlineView != null && offlineView.getPartitionSet().size() == NUM_OFFLINE_SEGMENTS
          && realtimeView != null;
    }, 30_000L, "Failed to find all OFFLINE segments in the ExternalView");
  }

  private Map<String, String> getStreamConfigs() {
    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put("streamType", "kafka");
    streamConfigs.put("stream.kafka.topic.name", "kafkaTopic");
    streamConfigs.put("stream.kafka.decoder.class.name",
        "org.apache.pinot.plugin.stream.kafka.KafkaAvroMessageDecoder");
    streamConfigs.put("stream.kafka.consumer.factory.class.name",
        "org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConsumerFactory");
    return streamConfigs;
  }

  /// Enabling the flag must actually build a manager and open the default store where configured.
  /// A failure to open degrades to statistics-disabled, which is otherwise silent.
  @Test
  public void testStatsAreEnabledAndTheStoreFileIsCreated() {
    assertNotNull(_brokerStarter.getStatsManager(), "Statistics were enabled but no manager was built");
    assertTrue(Files.exists(_statsDir.resolve("broker-stats.sqlite")),
        "The sqlite store should have been created under the configured pinot.broker.stats.dir");
  }

  /// The end-to-end assertion: segments announced through Helix reach the store by way of the
  /// routing manager's listener seam, with the document counts ZooKeeper carries.
  @Test
  public void testSegmentsAnnouncedThroughHelixReachTheStore() {
    BrokerTableStatsManager statsManager = _brokerStarter.getStatsManager();
    assertNotNull(statsManager);

    TestUtils.waitForCondition(aVoid -> {
      TableStatistics stats = statsManager.getTableStats(OFFLINE_TABLE_NAME);
      return stats != null && stats.getRowCount() == (long) NUM_OFFLINE_SEGMENTS * DOCS_PER_SEGMENT;
    }, 30_000L, "Offline segment statistics never reached the store");

    TableStatistics offlineStats = statsManager.getTableStats(OFFLINE_TABLE_NAME);
    assertNotNull(offlineStats);
    assertEquals(offlineStats.getRowCount(), (long) NUM_OFFLINE_SEGMENTS * DOCS_PER_SEGMENT);
    // Committed offline segments carry exact counts, so nothing should have downgraded them.
    assertEquals(offlineStats.getRowCountConfidence(), StatConfidence.EXACT);
  }

  /// The raw name resolves through the hybrid path, which needs the time boundary in epoch
  /// milliseconds. The time column here is in DAYS, which is what makes this worth asserting end to
  /// end: TimeBoundaryInfo carries the boundary in the column's own format, so a caller that parses
  /// that value directly gets 9 -- a number that parses cleanly and is not an instant.
  @Test
  public void testHybridTableResolvesByRawNameAtTheTimeBoundary() {
    BrokerTableStatsManager statsManager = _brokerStarter.getStatsManager();
    assertNotNull(statsManager);

    TestUtils.waitForCondition(aVoid -> statsManager.getTableStats(OFFLINE_TABLE_NAME) != null, 30_000L,
        "Offline segment statistics never reached the store");
    // Wait for the boundary explicitly. It is published asynchronously, and without this the
    // assertion below would silently be testing the no-boundary path most of the time.
    TestUtils.waitForCondition(
        aVoid -> _brokerStarter.getRoutingManager().getTimeBoundaryMs(OFFLINE_TABLE_NAME) != null, 30_000L,
        "The offline table never published a time boundary");

    Long boundaryMs = _brokerStarter.getRoutingManager().getTimeBoundaryMs(OFFLINE_TABLE_NAME);
    assertNotNull(boundaryMs);
    // Segments end at day 10 and the boundary trails by one day, so this is day 9 as an instant.
    assertEquals(boundaryMs.longValue(), TimeUnit.DAYS.toMillis(9),
        "The boundary must be epoch millis, not the formatted day number");

    TableStatistics hybridStats = statsManager.getTableStats(RAW_TABLE_NAME);
    assertNotNull(hybridStats, "The logical hybrid view should resolve from the raw table name");
    // The realtime side has no committed segments, so the offline rows are all there is.
    assertEquals(hybridStats.getRowCount(), (long) NUM_OFFLINE_SEGMENTS * DOCS_PER_SEGMENT);
  }

  /// The purge endpoint must be reachable on a running broker and must not touch a served table.
  /// This is the only coverage that the resource is registered and its manager binding resolves.
  @Test
  public void testPurgeEndpointLeavesServedTablesAlone()
      throws Exception {
    BrokerTableStatsManager statsManager = _brokerStarter.getStatsManager();
    assertNotNull(statsManager);
    TestUtils.waitForCondition(aVoid -> statsManager.getTableStats(OFFLINE_TABLE_NAME) != null, 30_000L,
        "Offline segment statistics never reached the store");

    String response =
        sendDeleteRequest("http://localhost:" + BROKER_QUERY_PORT + "/statistics/orphaned");
    assertEquals(response.trim(), "[]", "A broker serving every stored table has nothing to purge");
    assertNotNull(statsManager.getTableStats(OFFLINE_TABLE_NAME),
        "The purge must not drop a table this broker still serves");
  }

  @AfterClass
  public void tearDown() {
    _brokerStarter.stop();
    stopController();
    stopZk();
  }
}
