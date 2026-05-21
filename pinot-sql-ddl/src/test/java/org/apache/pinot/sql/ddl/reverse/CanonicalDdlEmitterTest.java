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
package org.apache.pinot.sql.ddl.reverse;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Golden-output unit tests for [CanonicalDdlEmitter]. Every test asserts the exact emitted
/// string so the canonical form is locked in: any drift requires updating both the emitter and
/// the golden expectation in the same PR.
public class CanonicalDdlEmitterTest {

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

    String expected =
        "CREATE TABLE events (\n"
            + "  id INT DIMENSION,\n"
            + "  name STRING DIMENSION\n"
            + ")\n"
            + "TABLE_TYPE = OFFLINE;\n";
    assertEquals(CanonicalDdlEmitter.emit(schema, config), expected);
  }

  @Test
  public void allColumnRolesAndDatetime() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("dim", DataType.STRING)
        .addMetric("sum", DataType.LONG)
        .addDateTime("ts", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setTimeColumnName("ts")
        .build();

    String emitted = CanonicalDdlEmitter.emit(schema, config);
    assertTrue(emitted.contains("dim STRING DIMENSION"), emitted);
    assertTrue(emitted.contains("sum LONG METRIC"), emitted);
    // DateTimeFieldSpec normalizes the format to "TIMESTAMP" when the column data type is
    // TIMESTAMP, so the canonical emission carries the short form regardless of what the
    // caller passed to addDateTime.
    assertTrue(emitted.contains(
        "ts TIMESTAMP DATETIME FORMAT 'TIMESTAMP' GRANULARITY '1:MILLISECONDS'"), emitted);
    assertTrue(emitted.contains("'timeColumnName' = 'ts'"), emitted);
  }

  @Test
  public void promotedPropertiesEmittedLexicographically() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", DataType.INT)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setBrokerTenant("tenantA")
        .setServerTenant("tenantB")
        .setNumReplicas(3)
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("30")
        .build();

    String emitted = CanonicalDdlEmitter.emit(schema, config);
    int brokerIdx = emitted.indexOf("'brokerTenant'");
    int replicationIdx = emitted.indexOf("'replication'");
    int retentionUnitIdx = emitted.indexOf("'retentionTimeUnit'");
    int serverIdx = emitted.indexOf("'serverTenant'");
    // Lexicographic order: brokerTenant < replication < retentionTimeUnit < serverTenant
    assertTrue(brokerIdx < replicationIdx, emitted);
    assertTrue(replicationIdx < retentionUnitIdx, emitted);
    assertTrue(retentionUnitIdx < serverIdx, emitted);
  }

  @Test
  public void streamConfigsRoundTripWithOriginalKeys() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("clicks")
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
        .setStreamConfigs(streamCfgs)
        .build();

    String emitted = CanonicalDdlEmitter.emit(schema, config);
    assertTrue(emitted.contains("'streamType' = 'kafka'"), emitted);
    assertTrue(emitted.contains("'stream.kafka.topic.name' = 'click_events'"), emitted);
    assertTrue(emitted.contains(
        "'stream.kafka.consumer.factory.class.name' = 'KafkaConsumerFactory'"), emitted);
    assertTrue(emitted.contains("'realtime.segment.flush.threshold.rows' = '500000'"), emitted);
  }

  @Test
  public void noDefaultsEmittedWhenAtNaturalDefault() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", DataType.INT)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        // replication defaults to "1" — should NOT appear in canonical output
        .build();

    String emitted = CanonicalDdlEmitter.emit(schema, config);
    assertFalse(emitted.contains("'replication'"), emitted);
    assertFalse(emitted.contains("'loadMode'"), emitted);
  }

  /// Regression: comparing default value vs natural default with Object.equals does reference
  /// equality on byte[], so a BYTES column at its natural default would always emit a redundant
  /// DEFAULT '<hex>' clause. The fix uses DataType.equals which delegates to Arrays.equals.
  @Test
  public void noDefaultEmittedForBytesAtNaturalDefault() {
    Schema schema = new Schema();
    schema.setSchemaName("t");
    schema.addField(new DimensionFieldSpec("blob", DataType.BYTES, true));

    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
    String emitted = CanonicalDdlEmitter.emit(schema, config);
    assertTrue(emitted.contains("blob BYTES DIMENSION"), emitted);
    assertFalse(emitted.contains("DEFAULT"),
        "BYTES column at natural default must not emit a DEFAULT clause; got:\n" + emitted);
  }

  /// Regression: BigDecimal.toString() can emit scientific notation (1E+30) which Calcite's
  /// Literal() rule does not accept. The fix routes BIG_DECIMAL defaults through toPlainString().
  @Test
  public void bigDecimalDefaultEmitsPlainString() {
    Schema schema = new Schema();
    schema.setSchemaName("t");
    MetricFieldSpec metric = new MetricFieldSpec("amount", DataType.BIG_DECIMAL,
        new BigDecimal("1E+30"));
    schema.addField(metric);

    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
    String emitted = CanonicalDdlEmitter.emit(schema, config);
    assertFalse(emitted.contains("E+") || emitted.contains("E-"),
        "BIG_DECIMAL default must not contain scientific notation; got:\n" + emitted);
    assertTrue(emitted.contains("1000000000000000000000000000000"),
        "expected plain-string form of 1E+30; got:\n" + emitted);
  }

  /// Regression: BIG_DECIMAL natural-default check must use compareTo rather than equals,
  /// so a stored BigDecimal("0.0") (scale 1) is treated as equivalent to BigDecimal.ZERO and
  /// canonical DDL elides the redundant DEFAULT clause.
  @Test
  public void bigDecimalAtNaturalDefaultDoesNotEmitDefault() {
    Schema schema = new Schema();
    schema.setSchemaName("t");
    MetricFieldSpec metric = new MetricFieldSpec("amount", DataType.BIG_DECIMAL,
        new BigDecimal("0.0"));
    schema.addField(metric);

    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
    String emitted = CanonicalDdlEmitter.emit(schema, config);
    assertFalse(emitted.contains("DEFAULT"),
        "BIG_DECIMAL at scale-shifted natural default must not emit DEFAULT; got:\n" + emitted);
  }

  /// Regression: BOOLEAN columns store defaults internally as Integer 0/1; canonical DDL
  /// must emit the SQL literal form (TRUE/FALSE) so the output is grammar-standard.
  @Test
  public void booleanDefaultEmittedAsSqlLiteral() {
    Schema schema = new Schema();
    schema.setSchemaName("t");
    DimensionFieldSpec dim = new DimensionFieldSpec("flag", DataType.BOOLEAN, true);
    dim.setDefaultNullValue(1);
    schema.addField(dim);

    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
    String emitted = CanonicalDdlEmitter.emit(schema, config);
    assertTrue(emitted.contains("DEFAULT TRUE"),
        "BOOLEAN default 1 must emit DEFAULT TRUE, got:\n" + emitted);
    assertFalse(emitted.contains("DEFAULT 1"),
        "must not emit raw integer encoding (DEFAULT 1); got:\n" + emitted);
  }

  /// Regression: BYTES non-natural-default emit must hex-encode the byte[] rather than fall
  /// through to value.toString() which would emit "[B@<addr>" identity-hash garbage.
  @Test
  public void bytesNonNaturalDefaultEmittedAsHex() {
    Schema schema = new Schema();
    schema.setSchemaName("t");
    DimensionFieldSpec dim = new DimensionFieldSpec("blob", DataType.BYTES, true);
    dim.setDefaultNullValue("deadbeef");
    schema.addField(dim);

    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
    String emitted = CanonicalDdlEmitter.emit(schema, config);
    assertTrue(emitted.contains("DEFAULT 'deadbeef'"),
        "BYTES default must be emitted as quoted hex string; got:\n" + emitted);
    assertFalse(emitted.contains("[B@"),
        "BYTES default must not leak the JVM byte[] identity-hash form; got:\n" + emitted);
  }

  /// Regression: TIMESTAMP DEFAULT emission must use UTC ISO-8601 form (Instant.toString)
  /// rather than java.sql.Timestamp.toString, which formats in the JVM's default time zone
  /// and would make canonical DDL emit different strings on different controllers.
  @Test
  public void timestampDefaultEmittedInUtcIso() {
    Schema schema = new Schema();
    schema.setSchemaName("t");
    DimensionFieldSpec dim = new DimensionFieldSpec("ts", DataType.TIMESTAMP, true);
    // 1700000000000 millis = 2023-11-14T22:13:20Z — pick a non-zero non-natural-default value
    // so the DEFAULT clause is actually emitted.
    dim.setDefaultNullValue(1700000000000L);
    schema.addField(dim);

    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
    String emitted = CanonicalDdlEmitter.emit(schema, config);
    // Instant.ofEpochMilli(1700000000000L).toString() is "2023-11-14T22:13:20Z" — UTC ISO-8601.
    assertTrue(emitted.contains("'2023-11-14T22:13:20Z'"),
        "TIMESTAMP default must emit UTC ISO-8601 form (Instant.toString); got:\n" + emitted);
  }

  @Test
  public void notNullAndDefaultEmittedExplicitly() {
    Schema schema = new Schema();
    schema.setSchemaName("t");
    DimensionFieldSpec dim = new DimensionFieldSpec("name", DataType.STRING, true, "N/A");
    dim.setNotNull(true);
    schema.addField(dim);
    MetricFieldSpec metric = new MetricFieldSpec("score", DataType.DOUBLE, 0.0);
    schema.addField(metric);

    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
    String emitted = CanonicalDdlEmitter.emit(schema, config);
    assertTrue(emitted.contains("name STRING NOT NULL DEFAULT 'N/A' DIMENSION"), emitted);
    // 0.0 IS the metric default for DOUBLE so DEFAULT clause should be elided
    assertTrue(emitted.contains("score DOUBLE METRIC"), emitted);
    assertFalse(emitted.contains("DEFAULT 0"), emitted);
  }

  @Test
  public void databaseQualifiedNameRendered() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", DataType.INT)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("analytics.events")
        .build();

    String emitted = CanonicalDdlEmitter.emit(schema, config, "analytics");
    assertTrue(emitted.startsWith("CREATE TABLE analytics.events ("), emitted);
  }

  @Test
  public void identifiersWithReservedNamesQuoted() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("orders")
        // METRIC is a Pinot DDL keyword; the column name must be quoted on emit so it round-trips.
        .addSingleValueDimension("metric", DataType.STRING)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("orders").build();
    String emitted = CanonicalDdlEmitter.emit(schema, config);
    assertTrue(emitted.contains("\"metric\" STRING DIMENSION"), emitted);
  }

  @Test
  public void taskConfigsEmittedAsPrefixedKeys() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", DataType.INT)
        .build();
    Map<String, Map<String, String>> tasks = new LinkedHashMap<>();
    Map<String, String> rtoCfg = new LinkedHashMap<>();
    rtoCfg.put("bucketTimePeriod", "1d");
    rtoCfg.put("maxNumRecordsPerSegment", "5000000");
    tasks.put("RealtimeToOfflineSegmentsTask", rtoCfg);
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setTaskConfig(new TableTaskConfig(tasks))
        .build();

    String emitted = CanonicalDdlEmitter.emit(schema, config);
    assertTrue(emitted.contains(
        "'task.RealtimeToOfflineSegmentsTask.bucketTimePeriod' = '1d'"), emitted);
    assertTrue(emitted.contains(
        "'task.RealtimeToOfflineSegmentsTask.maxNumRecordsPerSegment' = '5000000'"), emitted);
  }

  @Test
  public void customConfigEmittedVerbatim() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("t")
        .addSingleValueDimension("id", DataType.INT)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("t")
        .setCustomConfig(new TableCustomConfig(
            Collections.singletonMap("mySpecialKey", "someValue")))
        .build();

    String emitted = CanonicalDdlEmitter.emit(schema, config);
    assertTrue(emitted.contains("'mySpecialKey' = 'someValue'"), emitted);
  }

  @Test
  public void datetimeFieldRoundTripsFormatAndGranularity() {
    Schema schema = new Schema();
    schema.setSchemaName("t");
    schema.addField(new DateTimeFieldSpec("ts", DataType.LONG, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd",
        "1:DAYS"));
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
    String emitted = CanonicalDdlEmitter.emit(schema, config);
    assertTrue(emitted.contains(
        "ts LONG DATETIME FORMAT '1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd' GRANULARITY '1:DAYS'"),
        emitted);
  }

  @Test
  public void customConfigKeyShadowingPromotedKeyRejected() {
    // Regression: TableCustomConfig accepts arbitrary string keys, but when one collides with
    // a promoted/JSON-blob key the canonical DDL would emit it indistinguishably from the
    // promoted property — the round-trip would either silently misroute the value or fail
    // JSON deserialization. Reject up front so the user renames the colliding key.
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("t")
        .addSingleValueDimension("id", DataType.INT)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("t")
        .setCustomConfig(new TableCustomConfig(
            Collections.singletonMap("ingestionConfig", "anything")))
        .build();
    try {
      CanonicalDdlEmitter.emit(schema, config);
      org.testng.Assert.fail("Expected IllegalArgumentException for shadowing custom-config key");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("ingestionConfig"), expected.getMessage());
    }
  }

  @Test
  public void customConfigKeyShadowingTaskPrefixRejected() {
    // Same root-cause as above but for the prefix-routed paths: task.<type>.<key>, streamType,
    // stream.*, and realtime.* are all consumed by PropertyMapping's routing on re-parse, so
    // a TableCustomConfig entry under any of those prefixes would silently land in the wrong
    // TableConfig field. Reject so the user knows to rename.
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("t")
        .addSingleValueDimension("id", DataType.INT)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("t")
        .setCustomConfig(new TableCustomConfig(
            Collections.singletonMap("task.MyTask.foo", "bar")))
        .build();
    try {
      CanonicalDdlEmitter.emit(schema, config);
      org.testng.Assert.fail("Expected IllegalArgumentException for shadowing task.* key");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("task.MyTask.foo"), expected.getMessage());
    }
  }

  @Test
  public void customConfigKeyShadowingStreamTypeRejected() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("t")
        .addSingleValueDimension("id", DataType.INT)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("t")
        .setCustomConfig(new TableCustomConfig(
            Collections.singletonMap("streamType", "not-a-stream-config")))
        .build();
    try {
      CanonicalDdlEmitter.emit(schema, config);
      org.testng.Assert.fail("Expected IllegalArgumentException for shadowing streamType key");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("streamType"), expected.getMessage());
    }
  }

  @Test
  public void deterministicOutputAcrossRepeatedCalls() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("a", DataType.INT)
        .addSingleValueDimension("b", DataType.STRING)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setBrokerTenant("t1")
        .setServerTenant("t2")
        .setNumReplicas(2)
        .build();
    String first = CanonicalDdlEmitter.emit(schema, config);
    String second = CanonicalDdlEmitter.emit(schema, config);
    assertEquals(first, second, "Canonical emit must be deterministic");
  }
}
