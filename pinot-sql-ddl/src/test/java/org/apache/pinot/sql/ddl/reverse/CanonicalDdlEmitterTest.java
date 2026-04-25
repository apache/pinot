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

import java.util.Collections;
import org.apache.pinot.spi.config.table.TableConfig;
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


/**
 * Golden-output unit tests for {@link CanonicalDdlEmitter}. Every test asserts the exact emitted
 * string so the canonical form is locked in: any drift requires updating both the emitter and
 * the golden expectation in the same PR.
 */
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
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setTimeColumnName("ts")
        .build();

    String emitted = CanonicalDdlEmitter.emit(schema, config);
    assertTrue(emitted.contains("dim STRING DIMENSION"), emitted);
    assertTrue(emitted.contains("sum LONG METRIC"), emitted);
    assertTrue(emitted.contains(
        "ts LONG DATETIME FORMAT '1:MILLISECONDS:EPOCH' GRANULARITY '1:MILLISECONDS'"), emitted);
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
    java.util.Map<String, String> streamCfgs = new java.util.LinkedHashMap<>();
    streamCfgs.put("stream.kafka.topic.name", "click_events");
    streamCfgs.put("stream.kafka.consumer.factory.class.name", "KafkaConsumerFactory");
    streamCfgs.put("realtime.segment.flush.threshold.rows", "500000");
    TableConfig config = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("clicks")
        .setTimeColumnName("ts")
        .setStreamConfigs(streamCfgs)
        .build();

    String emitted = CanonicalDdlEmitter.emit(schema, config);
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

  /**
   * Regression: comparing default value vs natural default with Object.equals does reference
   * equality on byte[], so a BYTES column at its natural default would always emit a redundant
   * DEFAULT '<hex>' clause. The fix uses DataType.equals which delegates to Arrays.equals.
   */
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

  /**
   * Regression: BigDecimal.toString() can emit scientific notation (1E+30) which Calcite's
   * Literal() rule does not accept. The fix routes BIG_DECIMAL defaults through toPlainString().
   */
  @Test
  public void bigDecimalDefaultEmitsPlainString() {
    Schema schema = new Schema();
    schema.setSchemaName("t");
    MetricFieldSpec metric = new MetricFieldSpec("amount", DataType.BIG_DECIMAL,
        new java.math.BigDecimal("1E+30"));
    schema.addField(metric);

    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
    String emitted = CanonicalDdlEmitter.emit(schema, config);
    assertFalse(emitted.contains("E+") || emitted.contains("E-"),
        "BIG_DECIMAL default must not contain scientific notation; got:\n" + emitted);
    assertTrue(emitted.contains("1000000000000000000000000000000"),
        "expected plain-string form of 1E+30; got:\n" + emitted);
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
    java.util.Map<String, java.util.Map<String, String>> tasks = new java.util.LinkedHashMap<>();
    java.util.Map<String, String> rtoCfg = new java.util.LinkedHashMap<>();
    rtoCfg.put("bucketTimePeriod", "1d");
    rtoCfg.put("maxNumRecordsPerSegment", "5000000");
    tasks.put("RealtimeToOfflineSegmentsTask", rtoCfg);
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setTaskConfig(new org.apache.pinot.spi.config.table.TableTaskConfig(tasks))
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
        .setCustomConfig(new org.apache.pinot.spi.config.table.TableCustomConfig(
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
        .setCustomConfig(new org.apache.pinot.spi.config.table.TableCustomConfig(
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
    // Same root-cause as above but for the prefix-routed paths: task.<type>.<key>, stream.*,
    // and realtime.* are all consumed by PropertyMapping's prefix routing on re-parse, so
    // a TableCustomConfig entry under any of those prefixes would silently land in the wrong
    // TableConfig field. Reject so the user knows to rename.
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("t")
        .addSingleValueDimension("id", DataType.INT)
        .build();
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("t")
        .setCustomConfig(new org.apache.pinot.spi.config.table.TableCustomConfig(
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
