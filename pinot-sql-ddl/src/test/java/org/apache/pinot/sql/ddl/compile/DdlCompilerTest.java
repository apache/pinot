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
package org.apache.pinot.sql.ddl.compile;

import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/** End-to-end compiler tests: SQL → CompiledDdl → Schema + TableConfig. */
public class DdlCompilerTest {

  // -------------------------------------------------------------------------------------------
  // CREATE TABLE: schema mapping
  // -------------------------------------------------------------------------------------------

  @Test
  public void offlineMinimal() {
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE events (id INT, name STRING) TABLE_TYPE = OFFLINE");
    assertEquals(c.getTableConfig().getTableType(), TableType.OFFLINE);
    // TableConfig auto-appends the type suffix; schema name retains the bare table name.
    assertEquals(c.getTableConfig().getTableName(), "events_OFFLINE");
    assertEquals(c.getSchema().getSchemaName(), "events");
    assertEquals(c.getSchema().getDimensionNames().size(), 2);
    // Default replication is "1" from TableConfigBuilder.
    assertEquals(c.getTableConfig().getValidationConfig().getReplication(), "1");
  }

  @Test
  public void databaseQualifiedNamePreservedInTableConfig() {
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE analytics.events (id INT) TABLE_TYPE = OFFLINE");
    assertEquals(c.getDatabaseName(), "analytics");
    assertEquals(c.getTableConfig().getTableName(), "analytics.events_OFFLINE");
    // Schema name is the bare raw name; database scoping is on the table side.
    assertEquals(c.getSchema().getSchemaName(), "events");
  }

  @Test
  public void columnRolesProduceCorrectFieldSpecs() {
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE t ("
            + "  d1 STRING DIMENSION,"
            + "  m1 LONG METRIC,"
            + "  ts LONG DATETIME FORMAT '1:MILLISECONDS:EPOCH' GRANULARITY '1:MILLISECONDS'"
            + ") TABLE_TYPE = OFFLINE");

    Schema s = c.getSchema();
    FieldSpec d1 = s.getFieldSpecFor("d1");
    FieldSpec m1 = s.getFieldSpecFor("m1");
    FieldSpec ts = s.getFieldSpecFor("ts");
    assertTrue(d1 instanceof DimensionFieldSpec);
    assertTrue(m1 instanceof MetricFieldSpec);
    assertTrue(ts instanceof DateTimeFieldSpec);
    assertEquals(((DateTimeFieldSpec) ts).getFormat(), "1:MILLISECONDS:EPOCH");
    assertEquals(((DateTimeFieldSpec) ts).getGranularity(), "1:MILLISECONDS");
  }

  @Test
  public void unspecifiedRoleDefaultsToDimension() {
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE t (id INT, name STRING) TABLE_TYPE = OFFLINE");
    assertTrue(c.getSchema().getFieldSpecFor("id") instanceof DimensionFieldSpec);
    assertTrue(c.getSchema().getFieldSpecFor("name") instanceof DimensionFieldSpec);
  }

  @Test
  public void notNullSetsFieldSpecFlag() {
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE t (id INT NOT NULL) TABLE_TYPE = OFFLINE");
    assertTrue(c.getSchema().getFieldSpecFor("id").isNotNull());
  }

  @Test
  public void dataTypeMappingCoversCommonTypes() {
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE t ("
            + "  c_int INT,"
            + "  c_bigint BIGINT,"
            + "  c_long LONG,"
            + "  c_float FLOAT,"
            + "  c_double DOUBLE,"
            + "  c_decimal DECIMAL,"
            + "  c_bool BOOLEAN,"
            + "  c_str STRING,"
            + "  c_var VARCHAR,"
            + "  c_bin BYTES,"
            + "  c_ts TIMESTAMP"
            + ") TABLE_TYPE = OFFLINE");
    Schema s = c.getSchema();
    assertEquals(s.getFieldSpecFor("c_int").getDataType(), DataType.INT);
    assertEquals(s.getFieldSpecFor("c_bigint").getDataType(), DataType.LONG);
    assertEquals(s.getFieldSpecFor("c_long").getDataType(), DataType.LONG);
    assertEquals(s.getFieldSpecFor("c_float").getDataType(), DataType.FLOAT);
    assertEquals(s.getFieldSpecFor("c_double").getDataType(), DataType.DOUBLE);
    assertEquals(s.getFieldSpecFor("c_decimal").getDataType(), DataType.BIG_DECIMAL);
    assertEquals(s.getFieldSpecFor("c_bool").getDataType(), DataType.BOOLEAN);
    assertEquals(s.getFieldSpecFor("c_str").getDataType(), DataType.STRING);
    assertEquals(s.getFieldSpecFor("c_var").getDataType(), DataType.STRING);
    assertEquals(s.getFieldSpecFor("c_bin").getDataType(), DataType.BYTES);
    assertEquals(s.getFieldSpecFor("c_ts").getDataType(), DataType.TIMESTAMP);
  }

  // -------------------------------------------------------------------------------------------
  // CREATE TABLE: property mapping
  // -------------------------------------------------------------------------------------------

  @Test
  public void promotedPropertiesMapToTableConfigFields() {
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE events ("
            + "  ts LONG DATETIME FORMAT '1:MILLISECONDS:EPOCH' GRANULARITY '1:MILLISECONDS'"
            + ") TABLE_TYPE = OFFLINE PROPERTIES ("
            + "  'replication' = '3',"
            + "  'retentionTimeUnit' = 'DAYS',"
            + "  'retentionTimeValue' = '30',"
            + "  'brokerTenant' = 'tenantA',"
            + "  'serverTenant' = 'tenantB',"
            + "  'timeColumnName' = 'ts',"
            + "  'sortedColumn' = 'ts'"
            + ")");
    TableConfig cfg = c.getTableConfig();
    assertEquals(cfg.getValidationConfig().getReplication(), "3");
    assertEquals(cfg.getValidationConfig().getRetentionTimeUnit(), "DAYS");
    assertEquals(cfg.getValidationConfig().getRetentionTimeValue(), "30");
    assertEquals(cfg.getValidationConfig().getTimeColumnName(), "ts");
    assertEquals(cfg.getTenantConfig().getBroker(), "tenantA");
    assertEquals(cfg.getTenantConfig().getServer(), "tenantB");
    assertEquals(cfg.getIndexingConfig().getSortedColumn().get(0), "ts");
  }

  @Test
  public void streamPropertiesRoutedToStreamConfigsForRealtime() {
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE events ("
            + "  ts LONG DATETIME FORMAT '1:MILLISECONDS:EPOCH' GRANULARITY '1:MILLISECONDS'"
            + ") TABLE_TYPE = REALTIME PROPERTIES ("
            + "  'timeColumnName' = 'ts',"
            + "  'stream.kafka.topic.name' = 'orders',"
            + "  'stream.kafka.consumer.factory.class.name' = 'KafkaConsumerFactory',"
            + "  'realtime.segment.flush.threshold.rows' = '500000'"
            + ")");
    Map<String, String> stream = c.getTableConfig().getIndexingConfig().getStreamConfigs();
    assertNotNull(stream);
    // Both "stream.*" and "realtime.*" prefixes route to streamConfigs because that is where
    // Pinot actually reads them; routing elsewhere would make them silently inert.
    assertEquals(stream.get("stream.kafka.topic.name"), "orders");
    assertEquals(stream.get("stream.kafka.consumer.factory.class.name"), "KafkaConsumerFactory");
    assertEquals(stream.get("realtime.segment.flush.threshold.rows"), "500000");
  }

  @Test
  public void realtimePropertyOnOfflineTableRejected() {
    expectThrows(DdlCompilationException.class, () -> compileCreate(
        "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE PROPERTIES ("
            + "  'realtime.segment.flush.threshold.rows' = '500000')"));
  }

  @Test
  public void stringDefaultDoesNotLeakSqlQuotes() {
    // Regression for a bug where DEFAULT 'foo' produced a defaultNullValue of "'foo'" (with
    // surrounding quotes) because we were calling SqlNode.toString() instead of
    // SqlLiteral.getValue().toString(). The latter strips the SQL-wire quoting.
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE t (s STRING DEFAULT 'unknown') TABLE_TYPE = OFFLINE");
    Object defaultValue = c.getSchema().getFieldSpecFor("s").getDefaultNullValue();
    assertEquals(defaultValue, "unknown");
  }

  @Test
  public void numericDefaultRoundTripsCorrectly() {
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE t (m DOUBLE DEFAULT 0.0 METRIC) TABLE_TYPE = OFFLINE");
    Object defaultValue = c.getSchema().getFieldSpecFor("m").getDefaultNullValue();
    // FieldSpec coerces the string into the column's data type; the resulting value should be
    // numerically zero however it is represented.
    assertEquals(((Number) defaultValue).doubleValue(), 0.0);
  }

  @Test
  public void taskPropertiesRoutedToTaskConfig() {
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE PROPERTIES ("
            + "  'task.RealtimeToOfflineSegmentsTask.bucketTimePeriod' = '1d',"
            + "  'task.RealtimeToOfflineSegmentsTask.maxNumRecordsPerSegment' = '5000000',"
            + "  'task.SegmentRefreshTask.tableMaxNumTasks' = '5'"
            + ")");
    Map<String, Map<String, String>> tasks = c.getTableConfig().getTaskConfig().getTaskTypeConfigsMap();
    assertEquals(tasks.size(), 2);
    assertEquals(tasks.get("RealtimeToOfflineSegmentsTask").get("bucketTimePeriod"), "1d");
    assertEquals(tasks.get("RealtimeToOfflineSegmentsTask").get("maxNumRecordsPerSegment"), "5000000");
    assertEquals(tasks.get("SegmentRefreshTask").get("tableMaxNumTasks"), "5");
  }

  @Test
  public void unknownPropertiesPreservedInCustomConfig() {
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE PROPERTIES ("
            + "  'mySpecialKey' = 'someValue',"
            + "  'org.example.flag' = 'true'"
            + ")");
    Map<String, String> custom = c.getTableConfig().getCustomConfig().getCustomConfigs();
    assertNotNull(custom);
    assertEquals(custom.get("mySpecialKey"), "someValue");
    assertEquals(custom.get("org.example.flag"), "true");
  }

  // -------------------------------------------------------------------------------------------
  // CREATE TABLE: validation
  // -------------------------------------------------------------------------------------------

  @Test
  public void timeColumnMustReferenceDatetimeField() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class, () -> compileCreate(
        "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE PROPERTIES ('timeColumnName' = 'id')"));
    assertTrue(e.getMessage().contains("DATETIME"), e.getMessage());
  }

  @Test
  public void timeColumnMustExist() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class, () -> compileCreate(
        "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE PROPERTIES ('timeColumnName' = 'missing')"));
    assertTrue(e.getMessage().contains("missing"), e.getMessage());
  }

  @Test
  public void duplicateColumnRejected() {
    expectThrows(DdlCompilationException.class, () -> compileCreate(
        "CREATE TABLE t (id INT, ID STRING) TABLE_TYPE = OFFLINE"));
  }

  @Test
  public void duplicatePropertyRejected() {
    expectThrows(DdlCompilationException.class, () -> compileCreate(
        "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE PROPERTIES ("
            + "  'replication' = '3', 'replication' = '4')"));
  }

  @Test
  public void metricRoleRequiresNumericType() {
    expectThrows(DdlCompilationException.class, () -> compileCreate(
        "CREATE TABLE t (s STRING METRIC) TABLE_TYPE = OFFLINE"));
  }

  @Test
  public void reservedTableTypePropertyRejected() {
    expectThrows(DdlCompilationException.class, () -> compileCreate(
        "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE PROPERTIES ('tableType' = 'OFFLINE')"));
  }

  @Test
  public void streamPropertyOnOfflineTableRejected() {
    expectThrows(DdlCompilationException.class, () -> compileCreate(
        "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE PROPERTIES ("
            + "  'stream.kafka.topic.name' = 'orders')"));
  }

  @Test
  public void invalidTaskPropertyShapeRejected() {
    expectThrows(DdlCompilationException.class, () -> compileCreate(
        "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE PROPERTIES ('task.foo' = 'bar')"));
  }

  @Test
  public void replicationMustBeInteger() {
    expectThrows(DdlCompilationException.class, () -> compileCreate(
        "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE PROPERTIES ('replication' = 'abc')"));
  }

  @Test
  public void realtimeWithoutTimeColumnEmitsWarning() {
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE t (id INT) TABLE_TYPE = REALTIME");
    assertFalse(c.getWarnings().isEmpty());
    assertTrue(c.getWarnings().stream().anyMatch(w -> w.contains("timeColumnName")),
        "Expected timeColumnName warning, got: " + c.getWarnings());
  }

  // -------------------------------------------------------------------------------------------
  // DROP TABLE
  // -------------------------------------------------------------------------------------------

  @Test
  public void compileDropMinimal() {
    CompiledDdl c = DdlCompiler.compile("DROP TABLE events");
    assertEquals(c.getOperation(), DdlOperation.DROP_TABLE);
    CompiledDropTable d = (CompiledDropTable) c;
    assertEquals(d.getRawTableName(), "events");
    assertFalse(d.isIfExists());
    assertNull(d.getTableType());
  }

  @Test
  public void compileDropIfExistsWithType() {
    CompiledDropTable d = (CompiledDropTable) DdlCompiler.compile(
        "DROP TABLE IF EXISTS analytics.events TYPE OFFLINE");
    assertEquals(d.getDatabaseName(), "analytics");
    assertEquals(d.getRawTableName(), "events");
    assertTrue(d.isIfExists());
    assertEquals(d.getTableType(), TableType.OFFLINE);
  }

  // -------------------------------------------------------------------------------------------
  // PRIMARY KEY
  // -------------------------------------------------------------------------------------------

  @Test
  public void primaryKeyClauseSetsSchemaField() {
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE upsertTbl (id INT, val STRING) PRIMARY KEY (id) TABLE_TYPE = REALTIME");
    assertNotNull(c.getSchema().getPrimaryKeyColumns());
    assertEquals(c.getSchema().getPrimaryKeyColumns().size(), 1);
    assertEquals(c.getSchema().getPrimaryKeyColumns().get(0), "id");
  }

  @Test
  public void compositePrimaryKeyPreservesOrder() {
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE t (a INT, b STRING, c LONG) PRIMARY KEY (b, a) TABLE_TYPE = OFFLINE");
    assertEquals(c.getSchema().getPrimaryKeyColumns().size(), 2);
    assertEquals(c.getSchema().getPrimaryKeyColumns().get(0), "b");
    assertEquals(c.getSchema().getPrimaryKeyColumns().get(1), "a");
  }

  @Test
  public void noPrimaryKeyClauseLeavesPrimaryKeyColumnsNull() {
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE");
    assertNull(c.getSchema().getPrimaryKeyColumns());
  }

  @Test
  public void primaryKeyReferencingUnknownColumnThrows() {
    expectThrows(DdlCompilationException.class, () -> compileCreate(
        "CREATE TABLE t (id INT) PRIMARY KEY (nonexistent) TABLE_TYPE = OFFLINE"));
  }

  @Test
  public void replicasPerPartitionPropertyApplied() {
    CompiledCreateTable c = compileCreate(
        "CREATE TABLE t (id INT) TABLE_TYPE = REALTIME PROPERTIES ("
            + "  'replicasPerPartition' = '3')");
    assertEquals(c.getTableConfig().getValidationConfig().getReplicasPerPartition(), "3");
  }

  // -------------------------------------------------------------------------------------------
  // SHOW TABLES
  // -------------------------------------------------------------------------------------------

  @Test
  public void compileShowDefault() {
    CompiledShowTables s = (CompiledShowTables) DdlCompiler.compile("SHOW TABLES");
    assertNull(s.getDatabaseName());
  }

  @Test
  public void compileShowFromDatabase() {
    CompiledShowTables s = (CompiledShowTables) DdlCompiler.compile("SHOW TABLES FROM analytics");
    assertEquals(s.getDatabaseName(), "analytics");
  }

  // -------------------------------------------------------------------------------------------
  // DECIMAL precision warning
  // -------------------------------------------------------------------------------------------

  @Test
  public void decimalWithPrecisionScaleEmitsWarning() {
    CompiledCreateTable c = compileCreate("CREATE TABLE t (price DECIMAL(10,2)) TABLE_TYPE = OFFLINE");
    assertTrue(c.getWarnings().stream().anyMatch(w -> w.contains("DECIMAL")),
        "Expected DECIMAL precision warning, got: " + c.getWarnings());
  }

  @Test
  public void decimalWithoutPrecisionEmitsNoWarning() {
    CompiledCreateTable c = compileCreate("CREATE TABLE t (price DECIMAL) TABLE_TYPE = OFFLINE");
    assertTrue(c.getWarnings().stream().noneMatch(w -> w.contains("DECIMAL")),
        "Unexpected DECIMAL warning for bare DECIMAL: " + c.getWarnings());
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private static CompiledCreateTable compileCreate(String sql) {
    CompiledDdl c = DdlCompiler.compile(sql);
    assertEquals(c.getOperation(), DdlOperation.CREATE_TABLE);
    return (CompiledCreateTable) c;
  }
}
