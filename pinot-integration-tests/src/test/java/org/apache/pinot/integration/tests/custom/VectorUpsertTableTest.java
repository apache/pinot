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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.ExecutionStats;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// FULL-upsert vector search integration test.
///
/// Verifies that vector candidate generation considers only the upsert-valid documents (the query-scoped
/// doc-ids snapshot) before selecting top-K -- upsert-obsoleted physical versions must never consume
/// segment-level top-K candidate slots.
///
/// Dataset (2-D vectors, one Kafka partition, deterministic primary keys and comparison timestamps, stable
/// producer key, no scheduled compaction):
///
/// - A1-v1, A2-v1: vector `[1, 0]` -- later obsoleted, physically nearest to the query
/// - B1, B2: vectors `[0, 1]` and `[0, -1]` -- current
/// - A1-v2, A2-v2: vector `[-1, 0]` -- current, physically farthest from the query
/// - Query vector `[1, 0]`, K=2 -- expected current result: exactly B1 and B2
///
/// Two segment layouts are covered:
///
/// 1. All six physical records in one consuming (mutable) segment -- exercises the filter-aware mutable
///    HNSW index (near-real-time filtered search).
/// 2. Old A versions and B records sealed into an immutable segment, new A versions in the next consuming
///    segment -- exercises filtered ANN on the immutable HNSW index, where the obsolete rows are physically
///    nearest within the sealed segment and are invalidated by records in a different segment.
@Test(suiteName = "CustomClusterIntegrationTest")
public class VectorUpsertTableTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME_CONSUMING = "vectorUpsertConsuming";
  private static final String TABLE_NAME_SEALED = "vectorUpsertSealed";
  private static final String PRIMARY_KEY_COL = "entityId";
  private static final String VECTOR_COL = "embedding";
  private static final String TIME_COL = "ts";
  private static final String CSV_SCHEMA_HEADER = "entityId,embedding,ts";
  private static final String CSV_DELIMITER = ",";
  private static final String CSV_MULTI_VALUE_DELIMITER = ";";
  private static final String QUERY_VECTOR_LITERAL = "ARRAY[1.0, 0.0]";
  private static final int TOP_K = 2;

  // v1 records: A1/A2 at [1, 0] (nearest to the query), B1/B2 at [0, 1] and [0, -1]
  private static final List<String> INITIAL_RECORDS = List.of(
      "A1,1.0;0.0,1000",
      "A2,1.0;0.0,1001",
      "B1,0.0;1.0,1002",
      "B2,0.0;-1.0,1003");
  // v2 records: A1/A2 move to [-1, 0] (farthest from the query), obsoleting the v1 rows
  private static final List<String> UPDATE_RECORDS = List.of(
      "A1,-1.0;0.0,2000",
      "A2,-1.0;0.0,2001");

  @Override
  public String getTableName() {
    return TABLE_NAME_CONSUMING;
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  @Override
  protected int getNumKafkaPartitions() {
    return 1;
  }

  @Nullable
  @Override
  public String getTimeColumnName() {
    return TIME_COL;
  }

  @Override
  protected String getPartitionColumn() {
    return PRIMARY_KEY_COL;
  }

  @Override
  public Schema createSchema() {
    return createVectorSchema(TABLE_NAME_CONSUMING);
  }

  @Override
  public List<File> createAvroFiles() {
    // Data is pushed as CSV directly from the test methods
    return List.of();
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    return List.of(new FieldConfig.Builder(VECTOR_COL)
        .withIndexTypes(List.of(FieldConfig.IndexType.VECTOR))
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withProperties(Map.of(
            "vectorIndexType", "HNSW",
            "vectorDimension", "2",
            "vectorDistanceFunction", "EUCLIDEAN",
            "version", "1",
            // The filtered (upsert-aware) mutable search uses a near-real-time reader, but the unfiltered
            // path -- exercised by the skipUpsert control query -- only sees committed rows, and mutable
            // commits happen during add(). Commit every add so the control query is deterministic.
            "commitDocs", "1"))
        .build());
  }

  @Override
  @BeforeClass
  public void setUp()
      throws Exception {
    LOGGER.warn("Setting up integration test class: {}", getClass().getSimpleName());
    initControllerRequestURLBuilder();
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);
    setUpVectorUpsertTable(TABLE_NAME_CONSUMING);
    setUpVectorUpsertTable(TABLE_NAME_SEALED);
    LOGGER.warn("Finished setting up integration test class: {}", getClass().getSimpleName());
  }

  @Override
  @AfterClass
  public void tearDown()
      throws IOException {
    LOGGER.warn("Tearing down integration test class: {}", getClass().getSimpleName());
    dropRealtimeTable(TABLE_NAME_CONSUMING);
    deleteSchema(TABLE_NAME_CONSUMING);
    dropRealtimeTable(TABLE_NAME_SEALED);
    deleteSchema(TABLE_NAME_SEALED);
    FileUtils.deleteDirectory(_tempDir);
    LOGGER.warn("Finished tearing down integration test class: {}", getClass().getSimpleName());
  }

  private static String kafkaTopic(String tableName) {
    return tableName + "-kafka";
  }

  private Schema createVectorSchema(String tableName) {
    return new Schema.SchemaBuilder().setSchemaName(tableName)
        .addSingleValueDimension(PRIMARY_KEY_COL, FieldSpec.DataType.STRING)
        .addMultiValueDimension(VECTOR_COL, FieldSpec.DataType.FLOAT)
        .addDateTime(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(List.of(PRIMARY_KEY_COL))
        .build();
  }

  private void setUpVectorUpsertTable(String tableName)
      throws Exception {
    String kafkaTopicName = kafkaTopic(tableName);
    createSharedKafkaTopic(kafkaTopicName, getNumKafkaPartitions());

    Schema schema = createVectorSchema(tableName);
    addSchema(schema);

    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);
    csvDecoderProperties.put(StreamConfigProperties.constructStreamProperty("kafka",
        "decoder.prop.multiValueDelimiter"), CSV_MULTI_VALUE_DELIMITER);
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    TableConfig tableConfig = createCSVUpsertTableConfig(tableName, kafkaTopicName, getNumKafkaPartitions(),
        csvDecoderProperties, upsertConfig, PRIMARY_KEY_COL);
    addTableConfig(tableConfig);
  }

  private void pushRecords(String tableName, List<String> records)
      throws Exception {
    // Partition column index 0 (entityId) gives every entity a stable producer key
    ClusterIntegrationTestUtils.pushCsvIntoKafka(records, getSharedKafkaBrokerList(), kafkaTopic(tableName), 0,
        injectTombstones());
  }

  /// All six physical records live in a single consuming (mutable) segment: the filter-aware mutable HNSW
  /// index must restrict candidate generation to the upsert doc-ids snapshot through its near-real-time
  /// filtered search.
  @Test
  public void testVectorSearchWithAllRecordsInConsumingSegment()
      throws Exception {
    pushRecords(TABLE_NAME_CONSUMING, INITIAL_RECORDS);
    waitForPhysicalDocs(TABLE_NAME_CONSUMING, INITIAL_RECORDS.size());
    pushRecords(TABLE_NAME_CONSUMING, UPDATE_RECORDS);
    waitForPhysicalDocs(TABLE_NAME_CONSUMING, INITIAL_RECORDS.size() + UPDATE_RECORDS.size());

    assertVectorQueryResults(TABLE_NAME_CONSUMING, true);
  }

  /// The old A versions and the B records are sealed into an immutable segment (with a real HNSW index)
  /// before the new A versions arrive in the next consuming segment. Within the sealed segment the obsolete
  /// rows are physically nearest and are invalidated by records in a different segment, so this exercises
  /// filtered ANN with the required doc filter on the immutable HNSW reader.
  @Test
  public void testVectorSearchWithObsoleteRecordsInSealedSegment()
      throws Exception {
    pushRecords(TABLE_NAME_SEALED, INITIAL_RECORDS);
    waitForPhysicalDocs(TABLE_NAME_SEALED, INITIAL_RECORDS.size());

    // Seal A1-v1, A2-v1, B1, B2 into an immutable segment; a new consuming segment starts
    getOrCreateAdminClient().getTableClient().forceCommit(TABLE_NAME_SEALED);
    waitForSealedAndConsumingSegments(TABLE_NAME_SEALED);

    // The new A versions land in the new consuming segment
    pushRecords(TABLE_NAME_SEALED, UPDATE_RECORDS);
    waitForPhysicalDocs(TABLE_NAME_SEALED, INITIAL_RECORDS.size() + UPDATE_RECORDS.size());

    assertVectorQueryResults(TABLE_NAME_SEALED, false);
  }

  /// Asserts the vector query behavior. `vectorSimilarity` in a WHERE clause is a per-segment top-K
  /// filter, so only the single-segment layout can assert an exact-K filter result; the multi-segment
  /// layout asserts the canonical global-top-K query shape (filter + ORDER BY distance + LIMIT K) and
  /// that the current nearest entities survive segment-level candidate generation.
  private void assertVectorQueryResults(String tableName, boolean singleSegment)
      throws Exception {
    TreeSet<String> expectedCurrent = new TreeSet<>(List.of("B1", "B2"));

    // 1. Control (v1 engine): with skipUpsert=true the obsolete rows are visible and physically nearest,
    //    proving that an unfiltered top-K would be consumed by them
    setUseMultiStageQueryEngine(false);
    assertEquals(queryVectorFilterEntities(tableName, " OPTION(skipUpsert=true)"),
        new TreeSet<>(List.of("A1", "A2")),
        "skipUpsert control: the obsolete A-v1 rows must be the physically nearest to the query vector");

    for (boolean useMultiStageQueryEngine : new boolean[]{false, true}) {
      setUseMultiStageQueryEngine(useMultiStageQueryEngine);
      String engine = useMultiStageQueryEngine ? "multi-stage" : "single-stage";

      // 2. Canonical global top-K: vector filter + ORDER BY exact distance + LIMIT K. Without
      //    upsert-aware candidate generation, the sealed segment's candidate slots are consumed by the
      //    obsolete A-v1 rows, B1/B2 never become candidates, and this returns A1/A2 instead.
      String globalTopKQuery = String.format(
          "SELECT %s FROM %s WHERE vectorSimilarity(%s, %s, %d) ORDER BY euclideanDistance(%s, %s) ASC LIMIT %d",
          PRIMARY_KEY_COL, tableName, VECTOR_COL, QUERY_VECTOR_LITERAL, TOP_K, VECTOR_COL, QUERY_VECTOR_LITERAL,
          TOP_K);
      assertEquals(collectEntities(postQuery(globalTopKQuery)), expectedCurrent,
          "Global vector top-K on the " + engine + " engine must return the current nearest entities");

      // 3. Segment-level candidate correctness: the current nearest entities must survive the vector
      //    filter (each segment contributes its own top-K over valid docs)
      TreeSet<String> filterEntities = queryVectorFilterEntities(tableName, "");
      if (singleSegment) {
        assertEquals(filterEntities, expectedCurrent,
            "Single-segment vector filter on the " + engine + " engine must return exactly K current entities");
      } else {
        assertTrue(filterEntities.containsAll(expectedCurrent),
            "Vector filter on the " + engine + " engine must retain the current nearest entities, got: "
                + filterEntities);
      }

      // 4. The vector query must agree with the exact scalar-distance query over the upserted view
      String exactQuery = String.format(
          "SELECT %s FROM %s ORDER BY euclideanDistance(%s, %s) ASC LIMIT %d",
          PRIMARY_KEY_COL, tableName, VECTOR_COL, QUERY_VECTOR_LITERAL, TOP_K);
      assertEquals(collectEntities(postQuery(exactQuery)), expectedCurrent,
          "Exact scalar-distance top-K on the " + engine + " engine must return B1 and B2");
    }
    setUseMultiStageQueryEngine(false);
  }

  private TreeSet<String> queryVectorFilterEntities(String tableName, String querySuffix)
      throws Exception {
    String query = String.format("SELECT %s FROM %s WHERE vectorSimilarity(%s, %s, %d)%s",
        PRIMARY_KEY_COL, tableName, VECTOR_COL, QUERY_VECTOR_LITERAL, TOP_K, querySuffix);
    return collectEntities(postQuery(query));
  }

  private TreeSet<String> collectEntities(JsonNode queryResponse) {
    TreeSet<String> entities = new TreeSet<>();
    JsonNode rows = queryResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      entities.add(rows.get(i).get(0).asText());
    }
    return entities;
  }

  /// Polls until the physical (skipUpsert=true) doc count converges -- never rely on sleeps alone.
  private void waitForPhysicalDocs(String tableName, long expectedPhysicalDocs) {
    TestUtils.waitForCondition(
        () -> getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName + " OPTION(skipUpsert=true)")
            .getResultSet(0).getLong(0) == expectedPhysicalDocs,
        100L, 600_000L, "Failed to load " + expectedPhysicalDocs + " physical docs for table: " + tableName, null);
  }

  /// Polls until the table serves exactly one sealed and one consuming segment after a force-commit.
  private void waitForSealedAndConsumingSegments(String tableName) {
    TestUtils.waitForCondition(() -> {
      ExecutionStats executionStats =
          getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName).getExecutionStats();
      return executionStats.getNumSegmentsQueried() == 2 && executionStats.getNumConsumingSegmentsQueried() == 1;
    }, 100L, 600_000L, "Failed to reach one sealed plus one consuming segment for table: " + tableName, null);
  }
}
