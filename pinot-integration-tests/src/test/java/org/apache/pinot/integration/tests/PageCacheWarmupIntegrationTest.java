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
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.helix.ControllerRequestClient;
import org.apache.pinot.spi.config.table.PageCacheWarmupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for the page cache warmup feature.
 *
 * <p>It exercises the end-to-end "warmup on restart" flow:
 * <ol>
 *   <li>Enable {@code onRestart} warmup in the table config (round-trips through ZooKeeper via
 *       {@code TableConfigSerDeUtils}).</li>
 *   <li>Store warmup queries on the controller (written under the controller's
 *       {@code controller.page.cache.warmup.queries.dataDir}) and verify they can be read back.</li>
 *   <li>Restart the server that hosts the segments. During startup the server runs
 *       {@code startWarmupOnRestart}, which discovers a live controller via
 *       {@code HelixHelper.getControllerUrl(HelixManager)}, fetches the stored queries, and executes
 *       them against its resident segments.</li>
 *   <li>Assert that the host-level {@link ServerMeter#PAGE_CACHE_WARMUP_QUERIES} meter advanced and
 *       that the server still serves queries correctly afterwards.</li>
 * </ol>
 */
public class PageCacheWarmupIntegrationTest extends BaseClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PageCacheWarmupIntegrationTest.class);
  private static final String ORIGIN_COLUMN = "Origin";
  private static final String COUNT_COLUMN = "Count";
  private static final long NUM_DOCS = 1000L;
  // A bounded-but-realistic warmup duration so the restart path actually executes queries (a value of
  // 0 would make warmup an immediate no-op).
  private static final int WARMUP_DURATION_SECONDS = 30;
  // Cap the instance-level warmup window on the server so a hung warmup cannot stall the test for the
  // production default (180s). Comfortably larger than the per-table budget above. Declared as an int
  // to match the type of the underlying server config property.
  private static final int SERVER_WARMUP_DURATION_MS = 60_000;

  @Override
  protected String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(getTableName())
        .addSingleValueDimension(ORIGIN_COLUMN, FieldSpec.DataType.STRING)
        .addMetric(COUNT_COLUMN, FieldSpec.DataType.LONG)
        .build();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).build();
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    // Bound the instance-level warmup window so the test cannot block on the production default.
    serverConf.setProperty(CommonConstants.Server.MAX_PAGECACHE_WARMUP_DURATION_MS, SERVER_WARMUP_DURATION_MS);
  }

  private File createAvroFile()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("PageCacheWarmupRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(ORIGIN_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(COUNT_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null)));

    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      writer.create(avroSchema, avroFile);
      for (int i = 0; i < NUM_DOCS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(ORIGIN_COLUMN, "origin-" + (i % 10));
        record.put(COUNT_COLUMN, (long) i);
        writer.append(record);
      }
    }
    return avroFile;
  }

  /**
   * Builds a {@link ControllerRequestClient} on top of the test's controller URL builder. The base
   * class no longer exposes a shared client instance, so the warmup-query endpoints are reached
   * through a locally-constructed client.
   */
  private ControllerRequestClient getControllerRequestClient() {
    return new ControllerRequestClient(getControllerRequestURLBuilder(), HttpClient.getInstance(),
        getControllerRequestClientHeaders());
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster. The controller is configured with a page-cache warmup queries data dir
    // by default (ControllerTest#getDefaultControllerConfiguration sets
    // CONFIG_OF_PAGE_CACHE_WARMUP_QUERIES_DATA_DIR), so store/fetch works end-to-end.
    startZk();
    startController();
    startBroker();
    startServer();

    // Create and upload schema and table config.
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Build and upload a single segment from a self-contained Avro file.
    File avroFile = createAvroFile();
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFile, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    waitForAllDocsLoaded(60_000L);
  }

  @Test
  public void testWarmupOnRestart()
      throws Exception {
    String tableName = getTableName();
    List<String> queries = List.of(
        "SELECT COUNT(*) FROM " + tableName,
        "SELECT DISTINCT " + ORIGIN_COLUMN + " FROM " + tableName + " LIMIT 10",
        "SELECT SUM(" + COUNT_COLUMN + ") FROM " + tableName);

    ControllerRequestClient controllerRequestClient = getControllerRequestClient();
    try {
      // Step 1: Enable warmup on restart in the table config. This serializes pageCacheWarmupConfig to
      // ZooKeeper and is read back by the server's TableDataManager.
      PageCacheWarmupConfig.Spec restartSpec =
          new PageCacheWarmupConfig.Spec(true, WARMUP_DURATION_SECONDS, null, null);
      TableConfig tableConfig = getOfflineTableConfig();
      tableConfig.setPageCacheWarmupConfig(new PageCacheWarmupConfig(restartSpec, null));
      updateTableConfig(tableConfig);

      // Step 2: Store the warmup queries on the controller.
      assertTrue(controllerRequestClient.storePageCacheWarmupQueries(tableName, TableType.OFFLINE.name(), queries),
          "Failed to store page cache warmup queries");

      // Step 3: Verify the stored queries round-trip through the controller's data dir.
      List<String> storedQueries =
          controllerRequestClient.getPageCacheWarmupQueries(tableName, TableType.OFFLINE.name());
      assertEquals(storedQueries, queries, "Stored warmup queries do not match");

      // Step 4: Capture the baseline of the host-level warmup-queries meter before the restart. The
      // warmup executor reports on the global ServerMetrics singleton (registered by the first server
      // started in this JVM, backed by the shared in-process metrics registry), so we read it there.
      long baselineWarmupQueries = getWarmupQueriesMeterCount();

      // Step 5: Restart the server. On startup it runs startWarmupOnRestart(), which finds the resident
      // segments, discovers a live controller, fetches the stored queries, and executes them. start()
      // blocks through preServeQueries(), so warmup has completed by the time restartServers() returns.
      restartServers();

      // Step 6: The warmup should have executed at least the stored queries against the resident
      // segments, advancing the host-level meter. restartServers() already blocks through the server's
      // synchronous warmup, so a short bounded wait only guards against the meter mark landing a hair
      // after the warmup future completes; it should be satisfied almost immediately.
      int expectedWarmupQueries = queries.size();
      TestUtils.waitForCondition(
          aVoid -> getWarmupQueriesMeterCount() - baselineWarmupQueries >= expectedWarmupQueries, 10_000L,
          "PAGE_CACHE_WARMUP_QUERIES meter did not reach the expected count (" + expectedWarmupQueries
              + ") after restart; warmup-on-restart did not run the stored queries");
      long warmedUpQueries = getWarmupQueriesMeterCount() - baselineWarmupQueries;
      LOGGER.info("Page cache warmup executed {} queries on restart (expected >= {})", warmedUpQueries,
          expectedWarmupQueries);

      // Step 7: The server must still serve queries correctly after warming up.
      JsonNode response = postQuery("SELECT COUNT(*) FROM " + tableName);
      assertTrue(response.get("exceptions").isEmpty(), "Query returned exceptions after warmup: " + response);
      assertEquals(response.get("resultTable").get("rows").get(0).get(0).asLong(), NUM_DOCS,
          "Unexpected COUNT(*) after warmup");
    } finally {
      // Reset the warmup config and remove the stored queries so the cluster is left clean.
      TableConfig tableConfig = getOfflineTableConfig();
      tableConfig.setPageCacheWarmupConfig(null);
      updateTableConfig(tableConfig);
      controllerRequestClient.deletePageCacheWarmupQueries(tableName, TableType.OFFLINE.name());
    }
  }

  /**
   * Reads the accumulated host-level {@link ServerMeter#PAGE_CACHE_WARMUP_QUERIES} count from the
   * global server metrics. The warmup executor uses {@code addMeteredGlobalValue}, so the value is
   * read back through the matching global meter accessor.
   */
  private long getWarmupQueriesMeterCount() {
    return ServerMetrics.get().getMeteredValue(ServerMeter.PAGE_CACHE_WARMUP_QUERIES).count();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    try {
      dropOfflineTable(getTableName());
      stopServer();
      stopBroker();
      stopController();
      stopZk();
    } finally {
      FileUtils.deleteQuietly(_tempDir);
    }
  }
}
