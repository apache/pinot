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
package org.apache.pinot.sql.ddl.roundtrip;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.ddl.compile.CompiledCreateTable;
import org.apache.pinot.sql.ddl.compile.DdlCompiler;
import org.apache.pinot.sql.ddl.reverse.CanonicalDdlEmitter;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Round-trip suite: original (Schema, TableConfig) → canonical DDL → re-parse → re-compile →
/// round-tripped (Schema, TableConfig). Each test asserts the round-tripped pair is semantically
/// equivalent to the original — same Schema (column shape, datetime format, primary keys) and
/// same TableConfig fields.
///
/// Semantic equivalence is computed by comparing JSON serializations rather than direct
/// .equals(): TableConfig and Schema do not implement equals() reliably across all nested
/// configs, but their JSON representations are what eventually persist to ZK and what callers
/// actually compare.
///
/// Fixtures here are synthetic so the test is hermetic and does not depend on examples in
/// other modules. The set deliberately exercises every routing rule
/// (`streamType`, `stream.*`, `task.*`, JSON blob, custom config, promoted scalar, CSV list).
public class RoundTripTest {

  // -------------------------------------------------------------------------------------------
  // Round-trip cases
  // -------------------------------------------------------------------------------------------

  @Test
  public void minimalOfflineTable() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", DataType.INT)
        .addSingleValueDimension("name", DataType.STRING)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .build();
    assertRoundTrip(schema, config);
  }

  @Test
  public void offlineTableWithRetentionAndTenants() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", DataType.INT)
        .addMetric("score", DataType.DOUBLE)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setTimeColumnName("ts")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("30")
        .setNumReplicas(3)
        .setBrokerTenant("tenantA")
        .setServerTenant("tenantB")
        .build();
    assertRoundTrip(schema, config);
  }

  /// Regression: TIMESTAMP-typed time columns must round-trip through canonical emit, parse, and
  /// re-compile. DateTimeFieldSpec normalizes the format string to "TIMESTAMP" on construction
  /// (the format is implicit in the data type), so the emitted DDL carries `FORMAT 'TIMESTAMP'`
  /// and the re-parsed schema must reproduce the same normalized form.
  @Test
  public void offlineTableWithTimestampTimeColumn() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", DataType.INT)
        .addDateTime("ts", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setTimeColumnName("ts")
        .build();
    assertRoundTrip(schema, config);
  }

  /// Regression: BOOLEAN columns store defaults internally as Integer 0/1; the SQL literal
  /// `DEFAULT TRUE` must round-trip end-to-end. Calcite's `SqlLiteral.toValue()` returns the
  /// upper-case token "TRUE" for `BOOLEAN_TRUE`; `FieldSpec.setDefaultNullValue` then routes
  /// it through `DataType.BOOLEAN.convert(...)` (which calls `BooleanUtils.toInt`) to land
  /// the stored value as Integer 1. The emitter writes the SQL literal form back. This test
  /// locks in the full DDL → compile → emit → re-parse loop and also pins the natural-default
  /// elision behaviour for `DEFAULT FALSE` (BOOLEAN's stored natural default is Integer 0, so
  /// the canonical emitter intentionally omits the DEFAULT clause for `FALSE`).
  @Test
  public void offlineTableWithBooleanDefault() {
    String ddl = "CREATE TABLE flags (\n"
        + "  flag BOOLEAN NOT NULL DEFAULT TRUE DIMENSION,\n"
        + "  inactive BOOLEAN DEFAULT FALSE DIMENSION\n"
        + ")\n"
        + "TABLE_TYPE = OFFLINE;\n";
    CompiledCreateTable first = (CompiledCreateTable) DdlCompiler.compile(ddl);
    String firstEmit = CanonicalDdlEmitter.emit(first.getSchema(), first.getTableConfig());
    assertTrue(firstEmit.contains("flag BOOLEAN NOT NULL DEFAULT TRUE DIMENSION"),
        "Initial emit must preserve BOOLEAN NOT NULL DEFAULT TRUE; got:\n" + firstEmit);
    // DEFAULT FALSE is the natural default for BOOLEAN dimensions (stored as Integer 0), so the
    // emitter elides the DEFAULT clause. The column itself still appears.
    assertTrue(firstEmit.contains("inactive BOOLEAN DIMENSION"),
        "BOOLEAN column at natural default FALSE must emit without a DEFAULT clause; got:\n"
            + firstEmit);
    assertFalse(firstEmit.contains("inactive BOOLEAN DEFAULT FALSE"),
        "BOOLEAN natural default FALSE must not be emitted; got:\n" + firstEmit);
    // Idempotency: emit -> parse -> emit must produce the same canonical text.
    CompiledCreateTable second = (CompiledCreateTable) DdlCompiler.compile(firstEmit);
    String secondEmit = CanonicalDdlEmitter.emit(second.getSchema(), second.getTableConfig());
    assertEquals(secondEmit, firstEmit,
        "BOOLEAN DEFAULT canonical DDL must be idempotent across re-emit:\n" + firstEmit);
  }

  @Test
  public void offlineTableWithIndexingConfig() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("country", DataType.STRING)
        .addSingleValueDimension("city", DataType.STRING)
        .addMetric("amount", DataType.DOUBLE)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setSortedColumn("country")
        .setInvertedIndexColumns(Arrays.asList("city"))
        .setNoDictionaryColumns(Arrays.asList("amount"))
        .setBloomFilterColumns(Arrays.asList("country"))
        .setNullHandlingEnabled(true)
        .build();
    assertRoundTrip(schema, config);
  }

  @Test
  public void realtimeTableWithStreamConfigs() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("clicks")
        .addSingleValueDimension("user_id", DataType.STRING)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    Map<String, String> streamCfgs = new LinkedHashMap<>();
    streamCfgs.put("streamType", "kafka");
    streamCfgs.put("stream.kafka.topic.name", "click_events");
    streamCfgs.put("stream.kafka.consumer.factory.class.name", "KafkaConsumerFactory");
    streamCfgs.put("realtime.segment.flush.threshold.rows", "500000");
    TableConfig config = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("clicks")
        .setTimeColumnName("ts")
        .setNumReplicas(2)
        .setStreamConfigs(streamCfgs)
        .build();
    assertRoundTrip(schema, config);
  }

  @Test
  public void offlineTableWithTaskConfig() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", DataType.INT)
        .build();
    Map<String, Map<String, String>> tasks = new LinkedHashMap<>();
    Map<String, String> rto = new LinkedHashMap<>();
    rto.put("bucketTimePeriod", "1d");
    rto.put("maxNumRecordsPerSegment", "5000000");
    tasks.put("RealtimeToOfflineSegmentsTask", rto);
    Map<String, String> refresh = new LinkedHashMap<>();
    refresh.put("tableMaxNumTasks", "5");
    tasks.put("SegmentRefreshTask", refresh);
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setTaskConfig(new TableTaskConfig(tasks))
        .build();
    assertRoundTrip(schema, config);
  }

  @Test
  public void offlineTableWithCustomConfig() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("t")
        .addSingleValueDimension("id", DataType.INT)
        .build();
    Map<String, String> custom = new LinkedHashMap<>();
    custom.put("ourTeam.flag", "true");
    custom.put("internal.config.X", "value-x");
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("t")
        .setCustomConfig(new TableCustomConfig(custom))
        .build();
    assertRoundTrip(schema, config);
  }

  @Test
  public void offlineTableWithIngestionConfigJsonBlob() {
    // Ingestion config is a complex nested type with no first-class DDL clause; it round-trips
    // through PROPERTIES('ingestionConfig' = '<json>'). This proves the JSON-blob fallback
    // preserves structurally-rich configs without silent loss.
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", DataType.INT)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    BatchIngestionConfig batch = new BatchIngestionConfig(null, "APPEND", "DAILY");
    IngestionConfig ingestion = new IngestionConfig();
    ingestion.setBatchIngestionConfig(batch);
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setTimeColumnName("ts")
        .setIngestionConfig(ingestion)
        .build();
    assertRoundTrip(schema, config);
  }

  @Test
  public void columnDefaultsRoundTrip() {
    Schema schema = new Schema();
    schema.setSchemaName("t");
    DimensionFieldSpec name = new DimensionFieldSpec("name", DataType.STRING, true, "N/A");
    name.setNotNull(true);
    schema.addField(name);
    MetricFieldSpec score = new MetricFieldSpec("score", DataType.DOUBLE, 42.0);
    schema.addField(score);
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
    assertRoundTrip(schema, config);
  }

  @Test
  public void bytesMetricRoundTrips() {
    Schema schema = new Schema();
    schema.setSchemaName("t");
    schema.addField(new MetricFieldSpec("digest", DataType.BYTES));
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
    assertRoundTrip(schema, config);
  }

  @Test
  public void identifiersWithReservedNamesRoundTrip() {
    // Column named "metric" requires quoting on emit; round-trip must preserve the name.
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("orders")
        .addSingleValueDimension("metric", DataType.STRING)
        .addSingleValueDimension("dimension", DataType.STRING)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("orders").build();
    assertRoundTrip(schema, config);
  }

  @Test
  public void columnsNamedAfterDataTypeKeywordsRoundTrip() {
    // INT, STRING, BOOLEAN, etc. are data-type tokens consumed by the column-declaration
    // grammar; column names that match them must be quoted on emission so the re-parse treats
    // them as identifiers, not type tokens.
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("t")
        .addSingleValueDimension("int", DataType.INT)
        .addSingleValueDimension("string", DataType.STRING)
        .addSingleValueDimension("boolean", DataType.BOOLEAN)
        .addMetric("double", DataType.DOUBLE)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
    assertRoundTrip(schema, config);
  }

  @Test
  public void promotedScalarsAddedInSlice2RoundTrip() {
    // Regression for PropertyExtractor/PropertyMapping symmetry: every key the emitter writes
    // must have a matching forward-direction handler. This test exercises every promoted scalar
    // added in Slice 2 so any future extractor addition without a matching handler fails here.
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", DataType.INT)
        .addSingleValueDimension("country", DataType.STRING)
        .addSingleValueDimension("city", DataType.STRING)
        .addMetric("amount", DataType.DOUBLE)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setTimeColumnName("ts")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("60")
        .setNumReplicas(2)
        .setBrokerTenant("tenantA")
        .setServerTenant("tenantB")
        .setSortedColumn("country")
        .setInvertedIndexColumns(Arrays.asList("city"))
        .setNoDictionaryColumns(Arrays.asList("amount"))
        .setOnHeapDictionaryColumns(Arrays.asList("country"))
        .setVarLengthDictionaryColumns(Arrays.asList("city"))
        .setBloomFilterColumns(Arrays.asList("country"))
        .setRangeIndexColumns(Arrays.asList("amount"))
        .setNullHandlingEnabled(true)
        .setAggregateMetrics(true)
        .setPeerSegmentDownloadScheme("https")
        .setCrypterClassName("org.apache.pinot.crypter.NoOpCrypter")
        .setSegmentVersion("v3")
        .setDeletedSegmentsRetentionPeriod("14d")
        .setDescription("a kitchen-sink test table")
        .setTags(Arrays.asList("ourTeam", "metricsPipeline"))
        .build();
    assertRoundTrip(schema, config);
  }

  @Test
  public void multiValueDimensionRoundTrips() {
    // MV dimensions must survive SHOW CREATE TABLE: SchemaEmitter must emit DIMENSION ARRAY and
    // the compiler must recreate the MV field spec so isSingleValue=false is preserved.
    Schema schema = new Schema();
    schema.setSchemaName("t");
    schema.addField(new DimensionFieldSpec("tags", DataType.STRING, false));
    schema.addField(new DimensionFieldSpec("id", DataType.INT, true));
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
    assertRoundTrip(schema, config);
  }

  @Test
  public void primaryKeyRoundTrip() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("upsertTbl")
        .addSingleValueDimension("id", DataType.INT)
        .addSingleValueDimension("userId", DataType.STRING)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    schema.setPrimaryKeyColumns(Arrays.asList("id", "userId"));
    TableConfig config = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("upsertTbl")
        .setTimeColumnName("ts")
        .build();
    assertRoundTrip(schema, config);
  }

  @Test
  public void timeFieldSpecGranularityPreserved() {
    // Regression: SchemaEmitter was hardcoding "1:" as the granularity size instead of using
    // tgs.getTimeUnitSize(). Verify a 15-MINUTE TimeFieldSpec emits "15:MINUTES", not "1:MINUTES".
    // Note: TimeFieldSpec (legacy) is normalized to DateTimeFieldSpec on the re-parse side, so
    // this test only validates the emission step — it does not do a full schema round-trip.
    Schema schema = new Schema();
    schema.setSchemaName("events");
    schema.addField(new DimensionFieldSpec("id", DataType.INT, true));
    TimeGranularitySpec incoming = new TimeGranularitySpec(DataType.LONG, 15, TimeUnit.MINUTES, "incomingTime");
    TimeGranularitySpec outgoing = new TimeGranularitySpec(DataType.LONG, 15, TimeUnit.MINUTES, "ts");
    schema.addField(new TimeFieldSpec(incoming, outgoing));
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setTimeColumnName("ts")
        .build();
    String ddl = CanonicalDdlEmitter.emit(schema, config);
    assertNotNull(ddl);
    // The emitted granularity must reflect the actual timeUnitSize (15), not a hardcoded 1.
    assertTrue(ddl.contains("15:MINUTES"),
        "Expected granularity '15:MINUTES' in DDL but got:\n" + ddl);
    assertFalse(ddl.contains("1:MINUTES"),
        "Unexpected hardcoded '1:MINUTES' in DDL:\n" + ddl);
    // The emitted DDL must also be parseable/compileable without error.
    CompiledCreateTable compiled = (CompiledCreateTable) DdlCompiler.compile(ddl);
    assertNotNull(compiled, "Re-parsed DDL should compile");
  }

  @Test
  public void dateTimeFieldSpecRoundTrip() {
    // Regression for DateTimeFieldSpec: format string and granularity must survive the
    // emit → parse → compile round-trip unchanged.
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", DataType.INT)
        .addDateTime("eventTime", DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS")
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setTimeColumnName("eventTime")
        .build();
    assertRoundTrip(schema, config);
  }

  @Test
  public void replicasPerPartitionRoundTrip() {
    // Regression: replicasPerPartition was silently dropped by PropertyExtractor (it had no
    // handler) and therefore lost on SHOW CREATE TABLE. It must now survive the full round-trip.
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", DataType.INT)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("events")
        .build();
    config.getValidationConfig().setReplicasPerPartition("3");
    // Assert the emitted DDL text carries the property before doing the full semantic round-trip.
    String ddl = CanonicalDdlEmitter.emit(schema, config);
    assertTrue(ddl.contains("replicasPerPartition"),
        "Expected 'replicasPerPartition' in emitted DDL but got:\n" + ddl);
    assertRoundTrip(schema, config);
  }

  @Test
  public void unsupportedValidationConfigFieldFailsFastInsteadOfSilentLoss() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", DataType.INT)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .build();
    config.getValidationConfig().setReplacedSegmentsRetentionPeriod("3d");
    assertUnsupportedShowCreate(schema, config, "segmentsConfig.replacedSegmentsRetentionPeriod");
  }

  @Test
  public void unsupportedIndexingConfigFieldFailsFastInsteadOfSilentLoss() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", DataType.INT)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .build();
    config.getIndexingConfig().setNoDictionaryConfig(Collections.singletonMap("id", "RAW"));
    assertUnsupportedShowCreate(schema, config, "tableIndexConfig.noDictionaryConfig");
  }

  @Test
  public void unsupportedSchemaMetadataFailsFastInsteadOfSilentLoss() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", DataType.INT)
        .setEnableColumnBasedNullHandling(true)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .build();
    assertUnsupportedShowCreate(schema, config, "enableColumnBasedNullHandling");
  }

  @Test
  public void csvPropertyValueWithCommaFailsFastInsteadOfSilentSplit() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("country,city", DataType.STRING)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setSortedColumn("country,city")
        .build();
    assertUnsupportedShowCreate(schema, config, "sortedColumn");
  }

  @Test
  public void tagValueWithCommaFailsFastInsteadOfSilentSplit() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", DataType.INT)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setTags(Arrays.asList("team,one", "critical"))
        .build();
    assertUnsupportedShowCreate(schema, config, "tags");
  }

  // -------------------------------------------------------------------------------------------
  // Equivalence machinery
  // -------------------------------------------------------------------------------------------

  private static void assertUnsupportedShowCreate(Schema schema, TableConfig config,
      String expectedField) {
    try {
      CanonicalDdlEmitter.emit(schema, config);
      fail("Expected SHOW CREATE TABLE emission to reject unsupported field " + expectedField);
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains(expectedField),
          "Expected unsupported-field error to mention " + expectedField + " but got: "
              + expected.getMessage());
    }
  }

  /// Asserts that emit -> parse -> compile produces a semantically equivalent (schema, config).
  private static void assertRoundTrip(Schema originalSchema, TableConfig originalConfig) {
    String ddl = CanonicalDdlEmitter.emit(originalSchema, originalConfig);
    CompiledCreateTable round = (CompiledCreateTable) DdlCompiler.compile(ddl);
    assertNotNull(round, "Round-tripped DDL should compile: " + ddl);
    assertSchemaEquivalent(originalSchema, round.getSchema(), ddl);
    assertTableConfigEquivalent(originalConfig, round.getTableConfig(), ddl);

    // Idempotency: emit-parse-emit should yield the same canonical text.
    String secondEmit = CanonicalDdlEmitter.emit(round.getSchema(), round.getTableConfig());
    assertEquals(secondEmit, ddl, "Canonical DDL must be idempotent across round-trip:\n" + ddl);
  }

  private static void assertSchemaEquivalent(Schema a, Schema b, String ddl) {
    JsonNode aJson = stripVolatile(JsonUtils.objectToJsonNode(a));
    JsonNode bJson = stripVolatile(JsonUtils.objectToJsonNode(b));
    assertEquals(bJson, aJson, "Schema diverged on round-trip.\nDDL was:\n" + ddl
        + "\nExpected: " + aJson + "\nActual:   " + bJson);
  }

  private static void assertTableConfigEquivalent(TableConfig a, TableConfig b, String ddl) {
    JsonNode aJson = stripVolatile(JsonUtils.objectToJsonNode(a));
    JsonNode bJson = stripVolatile(JsonUtils.objectToJsonNode(b));
    assertEquals(bJson, aJson, "TableConfig diverged on round-trip.\nDDL was:\n" + ddl
        + "\nExpected: " + aJson + "\nActual:   " + bJson);
  }

  /// Removes fields that are not meaningful for semantic comparison. Empty maps in TableCustomConfig
  /// compare-equal whether the field is null, missing, or {}, so we strip them. Same for
  /// empty lists added by builders that are not user-meaningful.
  private static JsonNode stripVolatile(JsonNode node) {
    if (node == null || !node.isObject()) {
      return node;
    }
    ObjectNode obj = (ObjectNode) node;
    Iterator<Map.Entry<String, JsonNode>> it = obj.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> e = it.next();
      JsonNode v = e.getValue();
      if (v.isNull()) {
        it.remove();
      } else if (v.isObject() && v.size() == 0) {
        it.remove();
      } else if (v.isArray() && v.size() == 0) {
        it.remove();
      } else if (v.isObject()) {
        stripVolatile(v);
      }
    }
    // Re-check for now-empty objects after recursion
    Iterator<Map.Entry<String, JsonNode>> it2 = obj.fields();
    while (it2.hasNext()) {
      Map.Entry<String, JsonNode> e = it2.next();
      if (e.getValue().isObject() && e.getValue().size() == 0) {
        it2.remove();
      }
    }
    return obj;
  }
}
