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
package org.apache.pinot.integration.tests.logicaltable;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.primitives.Longs;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pinot.controller.api.resources.PauseStatusDetails;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.integration.tests.ClusterTest;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.LogicalTableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Chaos integration test for the Kafka partition subset feature.
 *
 * <p><b>Topology:</b>
 * <ul>
 *   <li>1 Kafka topic with {@value NUM_KAFKA_PARTITIONS} partitions</li>
 *   <li>1 "control" realtime table consuming <em>all</em> partitions (no subset filter), serving as ground truth</li>
 *   <li>3 "subset" realtime tables with <em>non-contiguous</em> partition assignments:
 *     <ul>
 *       <li>subset_0: partitions {0, 3}</li>
 *       <li>subset_1: partitions {1, 4}</li>
 *       <li>subset_2: partitions {2, 5}</li>
 *     </ul>
 *   </li>
 *   <li>1 logical table aggregating all three subset tables</li>
 * </ul>
 *
 * <p><b>Key invariant:</b> {@code COUNT(*) on logical table == COUNT(*) on control table}
 *
 * <p><b>Chaos scenarios tested:</b>
 * <ol>
 *   <li>Force-commit consuming segments on all subset tables</li>
 *   <li>Pause consumption, push more data, then resume – data must not be lost or duplicated</li>
 *   <li>Server restart – tables must recover and resume from their last committed offset</li>
 *   <li>Ingest a second batch after restart – counts must continue to match</li>
 * </ol>
 *
 * <p>The non-contiguous partition IDs (e.g., {0,3}) are intentional: they expose regressions
 * where code uses {@code getNumPartitions()} (returns a sequential 0..N-1 count) instead of
 * {@code getPartitionIds()} (returns the actual assigned partition ID set).
 */
public class KafkaPartitionSubsetChaosIntegrationTest extends BaseClusterIntegrationTest {

  // Kafka topic: 6 partitions to allow non-contiguous splits
  private static final String KAFKA_TOPIC = "partitionSubsetChaosTopic";
  private static final int NUM_KAFKA_PARTITIONS = 6;

  // Control table: ground truth – reads all 6 partitions
  private static final String CONTROL_TABLE = "controlAllPartitions";

  // Logical table: aggregates the three subset tables
  private static final String LOGICAL_TABLE = "logicalSubset";

  // Subset tables with non-contiguous partition assignments.
  // Using {0,3}, {1,4}, {2,5} deliberately skips sequential ordering to expose
  // getNumPartitions() vs getPartitionIds() bugs.
  private static final String[] SUBSET_TABLE_NAMES = {"subset0", "subset1", "subset2"};
  private static final int[][] SUBSET_PARTITION_ASSIGNMENTS = {{0, 3}, {1, 4}, {2, 5}};

  private static final long VERIFICATION_TIMEOUT_MS = 600_000L;
  private static final String DEFAULT_TENANT = "DefaultTenant";

  // Per-partition record counts – updated whenever we push data
  private final long[] _partitionRecordCounts = new long[NUM_KAFKA_PARTITIONS];

  // ---------------------------------------------------------------------------
  // ClusterTest overrides
  // ---------------------------------------------------------------------------

  @Override
  protected String getKafkaTopic() {
    return KAFKA_TOPIC;
  }

  @Override
  protected int getNumKafkaPartitions() {
    return NUM_KAFKA_PARTITIONS;
  }

  /** The "default" table name used by the base-class helper methods (schema creation, etc.). */
  @Override
  public String getTableName() {
    return CONTROL_TABLE;
  }

  @Override
  public String getHelixClusterName() {
    return "KafkaPartitionSubsetChaosIntegrationTest";
  }

  // ---------------------------------------------------------------------------
  // Suite lifecycle
  // ---------------------------------------------------------------------------

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    startZk();
    startController();
    startBroker();
    startServer();
    startKafka();

    List<File> avroFiles = getAllAvroFiles();

    // Set the sample file used by AvroFileSchemaKafkaAvroMessageDecoder (static, schema is shared)
    ClusterTest.AvroFileSchemaKafkaAvroMessageDecoder._avroFile = avroFiles.get(0);

    // ---- Control table: consumes all partitions ----
    Schema controlSchema = createSchema();
    addSchema(controlSchema);
    addTableConfig(buildRealtimeTableConfig(CONTROL_TABLE, null));

    // ---- Subset tables: each consumes a non-contiguous partition subset ----
    for (int i = 0; i < SUBSET_TABLE_NAMES.length; i++) {
      Schema schema = createSchema();
      schema.setSchemaName(SUBSET_TABLE_NAMES[i]);
      addSchema(schema);
      addTableConfig(buildRealtimeTableConfig(SUBSET_TABLE_NAMES[i], SUBSET_PARTITION_ASSIGNMENTS[i]));
    }

    // ---- Logical table ----
    Schema logicalSchema = createSchema();
    logicalSchema.setSchemaName(LOGICAL_TABLE);
    addSchema(logicalSchema);
    addLogicalTableConfig(LOGICAL_TABLE, buildSubsetPhysicalTableNames());

    // ---- Push initial data round-robin across all 6 partitions ----
    pushAvroRoundRobin(avroFiles, 0);

    // ---- Wait for all tables to catch up ----
    waitForCountInvariant(VERIFICATION_TIMEOUT_MS);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  // ---------------------------------------------------------------------------
  // Tests (run in declared order via priority)
  // ---------------------------------------------------------------------------

  /**
   * Sanity check: after initial ingestion the logical table and the control table must agree.
   */
  @Test(priority = 0)
  public void testCountInvariantAfterInitialIngestion() {
    assertCountInvariant();
  }

  /**
   * Force-commit consuming segments on each subset table and verify no data is lost or duplicated.
   *
   * <p>This exercises the path where the controller picks up consuming segments by partition ID.
   * A regression using {@code getNumPartitions()} would cause it to iterate partitions 0..1 for a
   * table assigned to e.g. {0,3} and miss partition 3 entirely.
   */
  @Test(priority = 1, dependsOnMethods = "testCountInvariantAfterInitialIngestion")
  public void testCountInvariantAfterForceCommit()
      throws Exception {
    for (String tableName : SUBSET_TABLE_NAMES) {
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      Set<String> consumingBefore = getConsumingSegments(realtimeTableName);
      if (consumingBefore.isEmpty()) {
        continue;
      }
      String jobId = forceCommit(realtimeTableName);
      waitForForceCommitCompletion(realtimeTableName, jobId, consumingBefore, 120_000L);
    }

    // Counts must be unchanged after force-commit
    assertCountInvariant();
  }

  /**
   * Pause consumption on all subset tables, push a second batch of records, then resume.
   *
   * <p>While consumption is paused the control table keeps ingesting, so the logical table will
   * temporarily fall behind. After resumption it must catch back up so that the invariant is
   * restored.
   */
  @Test(priority = 2, dependsOnMethods = "testCountInvariantAfterForceCommit")
  public void testCountInvariantAfterPauseResume()
      throws Exception {
    // Pause all subset tables
    for (String tableName : SUBSET_TABLE_NAMES) {
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      pauseConsumptionAndWait(realtimeTableName);
    }

    // Push a second batch – control table will consume it, subset tables will not (yet)
    List<File> avroFiles = getAllAvroFiles();
    long countBeforePush = totalExpectedCount();
    pushAvroRoundRobin(avroFiles, /* keyOffset= */ (int) countBeforePush);

    long expectedControlCount = totalExpectedCount();

    // Wait for control table to ingest the new records
    TestUtils.waitForCondition(
        aVoid -> getCurrentCountStarResult(CONTROL_TABLE) == expectedControlCount,
        1_000L, VERIFICATION_TIMEOUT_MS,
        "Control table did not reach expected count " + expectedControlCount + " after second push");

    // Resume all subset tables
    for (String tableName : SUBSET_TABLE_NAMES) {
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      resumeConsumptionAndWait(realtimeTableName);
    }

    // After resumption the invariant must be restored
    waitForCountInvariant(VERIFICATION_TIMEOUT_MS);
  }

  /**
   * Restart all servers and verify all tables recover and resume from their last committed offset.
   */
  @Test(priority = 3, dependsOnMethods = "testCountInvariantAfterPauseResume")
  public void testCountInvariantAfterServerRestart()
      throws Exception {
    long expectedTotal = totalExpectedCount();

    restartServers();

    // After restart, wait for all tables to reach their expected counts again
    TestUtils.waitForCondition(aVoid -> {
      try {
        return getCurrentCountStarResult(CONTROL_TABLE) == expectedTotal
            && getCurrentCountStarResult(LOGICAL_TABLE) == expectedTotal;
      } catch (Exception e) {
        return false;
      }
    }, 1_000L, VERIFICATION_TIMEOUT_MS, "Tables did not recover expected counts after server restart");

    assertCountInvariant();
  }

  /**
   * Push a third batch after server restart to confirm ongoing ingestion works correctly.
   *
   * <p>This catches regressions where partition state is re-initialised incorrectly after restart
   * (e.g., resetting offsets to 0 based on a sequential 0..N-1 scan instead of the configured
   * partition IDs).
   */
  @Test(priority = 4, dependsOnMethods = "testCountInvariantAfterServerRestart")
  public void testCountInvariantAfterIngestPostRestart()
      throws Exception {
    List<File> avroFiles = getAllAvroFiles();
    long countBeforePush = totalExpectedCount();
    pushAvroRoundRobin(avroFiles, /* keyOffset= */ (int) countBeforePush);

    waitForCountInvariant(VERIFICATION_TIMEOUT_MS);
  }

  // ---------------------------------------------------------------------------
  // Table-config builders
  // ---------------------------------------------------------------------------

  /**
   * Builds a realtime {@link TableConfig} for the given table name.
   *
   * @param tableName the raw (un-typed) table name
   * @param partitionIds partition IDs to assign, or {@code null} to consume all partitions
   */
  private TableConfig buildRealtimeTableConfig(String tableName, int[] partitionIds) {
    Map<String, String> streamConfigMap = new HashMap<>(getStreamConfigMap());

    if (partitionIds != null) {
      // Convert int[] to comma-separated string, e.g. "0,3"
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < partitionIds.length; i++) {
        if (i > 0) {
          sb.append(',');
        }
        sb.append(partitionIds[i]);
      }
      streamConfigMap.put("stream.kafka.partition.ids", sb.toString());
    }

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(Collections.singletonList(streamConfigMap)));

    return new TableConfigBuilder(TableType.REALTIME)
        .setTableName(tableName)
        .setTimeColumnName(getTimeColumnName())
        .setNumReplicas(getNumReplicas())
        .setBrokerTenant(DEFAULT_TENANT)
        .setServerTenant(DEFAULT_TENANT)
        .setIngestionConfig(ingestionConfig)
        .setNullHandlingEnabled(getNullHandlingEnabled())
        .build();
  }

  // ---------------------------------------------------------------------------
  // Logical table helpers
  // ---------------------------------------------------------------------------

  private List<String> buildSubsetPhysicalTableNames() {
    List<String> names = new ArrayList<>();
    for (String tableName : SUBSET_TABLE_NAMES) {
      names.add(TableNameBuilder.REALTIME.tableNameWithType(tableName));
    }
    return names;
  }

  private void addLogicalTableConfig(String logicalTableName, List<String> physicalTableNames)
      throws IOException {
    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    for (String physicalTableName : physicalTableNames) {
      physicalTableConfigMap.put(physicalTableName, new PhysicalTableConfig());
    }

    String refRealtimeTableName = physicalTableNames.stream()
        .filter(TableNameBuilder::isRealtimeTableResource)
        .findFirst()
        .orElse(null);

    LogicalTableConfig logicalTableConfig = new LogicalTableConfigBuilder()
        .setTableName(logicalTableName)
        .setBrokerTenant(DEFAULT_TENANT)
        .setRefRealtimeTableName(refRealtimeTableName)
        .setPhysicalTableConfigMap(physicalTableConfigMap)
        .build();

    String addUrl = _controllerRequestURLBuilder.forLogicalTableCreate();
    String resp = sendPostRequest(addUrl, logicalTableConfig.toSingleLineJsonString());
    assertEquals(resp, "{\"unrecognizedProperties\":{},\"status\":\"" + logicalTableName
        + " logical table successfully added.\"}");
  }

  // ---------------------------------------------------------------------------
  // Data push helpers
  // ---------------------------------------------------------------------------

  /**
   * Pushes all records from the given avro files to the Kafka topic in strict round-robin order
   * across all {@value NUM_KAFKA_PARTITIONS} partitions, starting the message key sequence at
   * {@code keyOffset}.
   *
   * <p>Each call atomically updates {@link #_partitionRecordCounts} so that
   * {@link #totalExpectedCount()} always reflects the total number of records in Kafka.
   */
  private void pushAvroRoundRobin(List<File> avroFiles, long keyOffset)
      throws Exception {
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaPort());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");

    long keySeq = keyOffset;
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(65536)) {

      for (File avroFile : avroFiles) {
        try (DataFileStream<GenericRecord> stream = AvroUtils.getAvroReader(avroFile)) {
          GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(stream.getSchema());
          for (GenericRecord record : stream) {
            int partition = (int) (keySeq % NUM_KAFKA_PARTITIONS);
            outputStream.reset();

            BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(outputStream, null);
            writer.write(record, encoder);
            encoder.flush();

            producer.send(new ProducerRecord<>(
                KAFKA_TOPIC, partition, Longs.toByteArray(keySeq), outputStream.toByteArray())).get();

            _partitionRecordCounts[partition]++;
            keySeq++;
          }
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Verification helpers
  // ---------------------------------------------------------------------------

  /** Returns the total number of records pushed across all partitions. */
  private long totalExpectedCount() {
    long total = 0;
    for (long count : _partitionRecordCounts) {
      total += count;
    }
    return total;
  }

  /**
   * Returns the expected record count for subset table {@code subsetIndex} based on its
   * partition assignment and the number of records pushed to each partition so far.
   */
  private long expectedSubsetCount(int subsetIndex) {
    long count = 0;
    for (int partition : SUBSET_PARTITION_ASSIGNMENTS[subsetIndex]) {
      count += _partitionRecordCounts[partition];
    }
    return count;
  }

  /**
   * Waits until the logical table and the control table both reflect all pushed data, and each
   * subset table reflects exactly the records pushed to its assigned partitions.
   */
  private void waitForCountInvariant(long timeoutMs) {
    long expectedTotal = totalExpectedCount();

    // Wait for control table
    TestUtils.waitForCondition(
        aVoid -> getCurrentCountStarResult(CONTROL_TABLE) == expectedTotal,
        1_000L, timeoutMs,
        "Control table did not reach expected count " + expectedTotal);

    // Wait for logical table
    TestUtils.waitForCondition(
        aVoid -> getCurrentCountStarResult(LOGICAL_TABLE) == expectedTotal,
        1_000L, timeoutMs,
        "Logical table did not reach expected count " + expectedTotal);

    // Wait for each individual subset table
    for (int i = 0; i < SUBSET_TABLE_NAMES.length; i++) {
      final long expectedSubset = expectedSubsetCount(i);
      final String subsetTable = SUBSET_TABLE_NAMES[i];
      TestUtils.waitForCondition(
          aVoid -> getCurrentCountStarResult(subsetTable) == expectedSubset,
          1_000L, timeoutMs,
          "Subset table " + subsetTable + " did not reach expected count " + expectedSubset);
    }
  }

  /**
   * Hard assertion (no waiting) that all count invariants currently hold.
   */
  private void assertCountInvariant() {
    long expectedTotal = totalExpectedCount();
    long controlCount = getCurrentCountStarResult(CONTROL_TABLE);
    long logicalCount = getCurrentCountStarResult(LOGICAL_TABLE);

    assertEquals(controlCount, expectedTotal,
        "Control table count mismatch. Expected: " + expectedTotal + ", actual: " + controlCount);
    assertEquals(logicalCount, expectedTotal,
        "Logical table count mismatch. Expected: " + expectedTotal + ", actual: " + logicalCount);

    for (int i = 0; i < SUBSET_TABLE_NAMES.length; i++) {
      long expectedSubset = expectedSubsetCount(i);
      long actualSubset = getCurrentCountStarResult(SUBSET_TABLE_NAMES[i]);
      assertEquals(actualSubset, expectedSubset,
          "Subset table " + SUBSET_TABLE_NAMES[i] + " count mismatch."
              + " Expected: " + expectedSubset + ", actual: " + actualSubset);
    }
  }

  // ---------------------------------------------------------------------------
  // Force-commit helpers
  // ---------------------------------------------------------------------------

  private String forceCommit(String realtimeTableName)
      throws Exception {
    String response = sendPostRequest(_controllerRequestURLBuilder.forTableForceCommit(realtimeTableName), null);
    return JsonUtils.stringToJsonNode(response).get("forceCommitJobId").asText();
  }

  private void waitForForceCommitCompletion(String realtimeTableName, String jobId,
      Set<String> consumingSegmentsBefore, long timeoutMs) {
    TestUtils.waitForCondition(aVoid -> {
      try {
        if (!isForceCommitJobCompleted(jobId)) {
          return false;
        }
        // Verify that the previously-consuming segments are now DONE
        for (String seg : consumingSegmentsBefore) {
          var meta = _helixResourceManager.getSegmentZKMetadata(realtimeTableName, seg);
          assertNotNull(meta);
          assertEquals(meta.getStatus(), CommonConstants.Segment.Realtime.Status.DONE);
        }
        return true;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, timeoutMs, "Force commit did not complete for " + realtimeTableName);
  }

  private boolean isForceCommitJobCompleted(String jobId)
      throws Exception {
    String resp = sendGetRequest(_controllerRequestURLBuilder.forForceCommitJobStatus(jobId));
    JsonNode status = JsonUtils.stringToJsonNode(resp);
    assertEquals(status.get("jobType").asText(), "FORCE_COMMIT");

    Set<String> pending = new HashSet<>();
    for (JsonNode elem : status.get(CommonConstants.ControllerJob.CONSUMING_SEGMENTS_YET_TO_BE_COMMITTED_LIST)) {
      pending.add(elem.asText());
    }
    return pending.isEmpty();
  }

  private Set<String> getConsumingSegments(String realtimeTableName) {
    IdealState idealState = _helixResourceManager.getTableIdealState(realtimeTableName);
    assertNotNull(idealState);
    Set<String> consuming = new HashSet<>();
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      if (entry.getValue().containsValue(CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING)) {
        consuming.add(entry.getKey());
      }
    }
    return consuming;
  }

  // ---------------------------------------------------------------------------
  // Pause / resume helpers
  // ---------------------------------------------------------------------------

  private void pauseConsumptionAndWait(String realtimeTableName)
      throws Exception {
    getControllerRequestClient().pauseConsumption(realtimeTableName);
    // After a successful pause no segments should remain in CONSUMING state
    TestUtils.waitForCondition(aVoid -> {
      try {
        PauseStatusDetails details = getControllerRequestClient().getPauseStatusDetails(realtimeTableName);
        return details != null && details.getPauseFlag()
            && (details.getConsumingSegments() == null || details.getConsumingSegments().isEmpty());
      } catch (Exception e) {
        return false;
      }
    }, 500L, 30_000L, "Consumption did not pause for " + realtimeTableName);
  }

  private void resumeConsumptionAndWait(String realtimeTableName)
      throws Exception {
    getControllerRequestClient().resumeConsumption(realtimeTableName);
    TestUtils.waitForCondition(aVoid -> {
      try {
        PauseStatusDetails details = getControllerRequestClient().getPauseStatusDetails(realtimeTableName);
        return details != null && !details.getPauseFlag();
      } catch (Exception e) {
        return false;
      }
    }, 500L, 30_000L, "Consumption did not resume for " + realtimeTableName);
  }
}
