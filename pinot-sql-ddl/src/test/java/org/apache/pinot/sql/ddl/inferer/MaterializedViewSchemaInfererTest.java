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
package org.apache.pinot.sql.ddl.inferer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.ddl.compile.DdlCompilationException;
import org.apache.pinot.sql.ddl.resolved.ColumnRole;
import org.apache.pinot.sql.ddl.resolved.ResolvedColumnDefinition;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Unit tests for [MaterializedViewSchemaInferer]. The inferer is deliberately narrow:
/// data type comes from Calcite's validated row type, the time column is pinned to
/// canonical TIMESTAMP, and recognised aggregations use the MV catalog (so sketches
/// land as BYTES). Non-recognised shapes flow through as DIMENSION with the Calcite
/// type; users who need a different role / format must use the explicit column list.
public class MaterializedViewSchemaInfererTest {

  // ---------------------------------------------------------------------------------------------
  // Happy paths
  // ---------------------------------------------------------------------------------------------

  @Test
  public void passthroughColumnTakesCalciteType() {
    Schema source = new Schema.SchemaBuilder()
        .setSchemaName("src")
        .addDateTime("ts", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:DAYS")
        .addSingleValueDimension("city", DataType.STRING)
        .build();
    List<ResolvedColumnDefinition> cols = infer(
        "SELECT ts, city AS region FROM src", "ts", source);

    assertEquals(cols.size(), 2);

    ResolvedColumnDefinition tsCol = cols.get(0);
    assertEquals(tsCol.getName(), "ts");
    assertEquals(tsCol.getRole(), ColumnRole.DATETIME,
        "The MV time column is always DATETIME / TIMESTAMP regardless of SELECT shape.");
    assertEquals(tsCol.getDataType(), DataType.TIMESTAMP);
    assertEquals(tsCol.getDateTimeFormat(), "1:MILLISECONDS:TIMESTAMP");
    assertEquals(tsCol.getDateTimeGranularity(), "1:MILLISECONDS");

    ResolvedColumnDefinition city = cols.get(1);
    assertEquals(city.getName(), "region", "AS alias must be the output column name");
    assertEquals(city.getRole(), ColumnRole.DIMENSION,
        "Non-time / non-aggregation columns default to DIMENSION.");
    assertEquals(city.getDataType(), DataType.STRING, "Calcite-derived data type");
    assertTrue(city.isNotNull(), "Inferred columns are NOT NULL by default.");
    assertNull(city.getDefaultValue(), "Inferred columns have no default.");
  }

  @Test
  public void timeColumnAlwaysCanonicalTimestamp() {
    // The time column is the canonical TIMESTAMP regardless of how the SELECT derives it.
    Schema source = new Schema.SchemaBuilder()
        .setSchemaName("src")
        .addDateTime("DaysSinceEpoch", DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();
    List<ResolvedColumnDefinition> cols = infer(
        "SELECT DaysSinceEpoch * 86400000 AS ts FROM src", "ts", source);

    assertEquals(cols.size(), 1);
    ResolvedColumnDefinition ts = cols.get(0);
    assertEquals(ts.getRole(), ColumnRole.DATETIME);
    assertEquals(ts.getDataType(), DataType.TIMESTAMP);
    assertEquals(ts.getDateTimeFormat(), "1:MILLISECONDS:TIMESTAMP");
    assertEquals(ts.getDateTimeGranularity(), "1:MILLISECONDS");
    assertTrue(ts.isNotNull());
  }

  @Test
  public void aggregationsLandAsCatalogTypes() {
    Schema source = new Schema.SchemaBuilder()
        .setSchemaName("src")
        .addDateTime("ts", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:DAYS")
        .addMetric("amount", DataType.DOUBLE)
        .build();
    List<ResolvedColumnDefinition> cols = infer(
        "SELECT ts, SUM(amount) AS total, COUNT(*) AS cnt FROM src GROUP BY ts",
        "ts", source);

    assertEquals(cols.size(), 3);

    ResolvedColumnDefinition total = cols.get(1);
    assertEquals(total.getName(), "total");
    assertEquals(total.getRole(), ColumnRole.METRIC);
    assertEquals(total.getDataType(), DataType.DOUBLE,
        "SUM must land as DOUBLE per the aggregation catalog");

    ResolvedColumnDefinition cnt = cols.get(2);
    assertEquals(cnt.getName(), "cnt");
    assertEquals(cnt.getRole(), ColumnRole.METRIC);
    assertEquals(cnt.getDataType(), DataType.LONG, "COUNT must land as LONG per the catalog");
  }

  /// The most subtle inferer contract — sketches must serialize as BYTES end-to-end, NOT
  /// the STRING that either engine surfaces. Pinned here so a future refactor that mistakes
  /// "engine return type" for "MV storage type" cannot silently regress correctness.
  @Test
  public void sketchAggregationLandsAsBytes() {
    Schema source = new Schema.SchemaBuilder()
        .setSchemaName("src")
        .addDateTime("ts", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:DAYS")
        .addSingleValueDimension("user_id", DataType.LONG)
        .build();
    List<ResolvedColumnDefinition> cols = infer(
        "SELECT ts, DISTINCTCOUNTRAWHLL(user_id) AS u_hll FROM src GROUP BY ts",
        "ts", source);

    assertEquals(cols.size(), 2);
    ResolvedColumnDefinition sketch = cols.get(1);
    assertEquals(sketch.getName(), "u_hll");
    assertEquals(sketch.getRole(), ColumnRole.METRIC);
    assertEquals(sketch.getDataType(), DataType.BYTES,
        "DISTINCTCOUNTRAWHLL must serialize as BYTES so the MV rewrite engine can re-aggregate "
            + "the raw sketch on the query side. STRING would silently break re-aggregation.");
  }

  @Test
  public void columnOrderMirrorsSelectListOrder() {
    Schema source = new Schema.SchemaBuilder()
        .setSchemaName("src")
        .addDateTime("ts", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:DAYS")
        .addSingleValueDimension("a", DataType.STRING)
        .addSingleValueDimension("b", DataType.STRING)
        .addSingleValueDimension("c", DataType.STRING)
        .build();
    List<ResolvedColumnDefinition> cols = infer(
        "SELECT c, a, ts, b FROM src", "ts", source);

    assertEquals(cols.size(), 4);
    assertEquals(cols.get(0).getName(), "c");
    assertEquals(cols.get(1).getName(), "a");
    assertEquals(cols.get(2).getName(), "ts");
    assertEquals(cols.get(3).getName(), "b",
        "Column order must mirror SELECT-list order so SHOW CREATE round-trips stably.");
  }

  // ---------------------------------------------------------------------------------------------
  // Validation rejections
  // ---------------------------------------------------------------------------------------------

  @Test
  public void selectStarRejectedWithRecoveryGuidance() {
    Schema source = new Schema.SchemaBuilder()
        .setSchemaName("src")
        .addDateTime("ts", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:DAYS")
        .build();
    DdlCompilationException ex = expectThrows(DdlCompilationException.class,
        () -> infer("SELECT * FROM src", "ts", source));
    assertTrue(ex.getMessage().contains("SELECT *"), ex.getMessage());
    assertTrue(ex.getMessage().contains("explicit"), ex.getMessage());
  }

  @Test
  public void unaliasedAggregationRejectedAsDdlCompilationException() {
    // RequestUtils.extractAliasOrIdentifierName throws IllegalStateException for non-bare
    // SELECT items without an AS alias. The inferer must translate that into a user-facing
    // DdlCompilationException so the controller renders a 400 with guidance, not a 500.
    Schema source = new Schema.SchemaBuilder()
        .setSchemaName("src")
        .addDateTime("ts", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:DAYS")
        .addMetric("amount", DataType.DOUBLE)
        .build();
    DdlCompilationException ex = expectThrows(DdlCompilationException.class,
        () -> infer("SELECT ts, SUM(amount) FROM src GROUP BY ts", "ts", source));
    assertTrue(ex.getMessage().contains("AS alias"), ex.getMessage());
  }

  @Test
  public void missingTimeColumnPropertyRejected() {
    Schema source = new Schema.SchemaBuilder()
        .setSchemaName("src")
        .addDateTime("ts", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:DAYS")
        .build();
    DdlCompilationException ex = expectThrows(DdlCompilationException.class,
        () -> inferWithProperties("SELECT ts FROM src",
            Collections.singletonMap("bucketTimePeriod", "1d"), source));
    assertTrue(ex.getMessage().contains("timeColumnName"), ex.getMessage());
  }

  @Test
  public void timeColumnNotProducedBySelectRejected() {
    Schema source = new Schema.SchemaBuilder()
        .setSchemaName("src")
        .addDateTime("ts", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:DAYS")
        .addSingleValueDimension("city", DataType.STRING)
        .build();
    // timeColumnName='bucket' but no SELECT alias produces it.
    DdlCompilationException ex = expectThrows(DdlCompilationException.class,
        () -> infer("SELECT ts, city FROM src", "bucket", source));
    assertTrue(ex.getMessage().contains("bucket"), ex.getMessage());
  }

  @Test
  public void duplicateColumnAliasRejected() {
    Schema source = new Schema.SchemaBuilder()
        .setSchemaName("src")
        .addDateTime("ts", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:DAYS")
        .addSingleValueDimension("a", DataType.STRING)
        .addSingleValueDimension("b", DataType.STRING)
        .build();
    DdlCompilationException ex = expectThrows(DdlCompilationException.class,
        () -> infer("SELECT ts, a AS dup, b AS dup FROM src", "ts", source));
    assertTrue(ex.getMessage().contains("duplicate"), ex.getMessage());
  }

  // ---------------------------------------------------------------------------------------------
  // Calcite validation surface — proves the QueryEnvironment is being consulted
  // ---------------------------------------------------------------------------------------------

  @Test
  public void calciteValidatorCatchesUnknownColumn() {
    Schema source = new Schema.SchemaBuilder()
        .setSchemaName("src")
        .addDateTime("ts", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:DAYS")
        .addSingleValueDimension("city", DataType.STRING)
        .build();
    // 'no_such_column' does not exist in the source. Calcite's validator must catch this
    // at DDL-compile time rather than letting it pass to MV-create time.
    DdlCompilationException ex = expectThrows(DdlCompilationException.class,
        () -> infer("SELECT ts, no_such_column FROM src", "ts", source));
    assertTrue(ex.getMessage().toLowerCase().contains("no_such_column")
        || ex.getMessage().toLowerCase().contains("not found")
        || ex.getMessage().toLowerCase().contains("calcite"),
        "Calcite validator must surface the unknown column; got: " + ex.getMessage());
  }

  // ---------------------------------------------------------------------------------------------
  // Wiring errors
  // ---------------------------------------------------------------------------------------------

  @Test
  public void missingTableCacheProducesActionableError() {
    MaterializedViewInferenceInput input = new MaterializedViewInferenceInput(
        "SELECT ts FROM src", null, "mv",
        Collections.singletonMap("timeColumnName", "ts"),
        /* tableCache = */ null);
    DdlCompilationException ex = expectThrows(DdlCompilationException.class,
        () -> MaterializedViewSchemaInferer.infer(input));
    assertTrue(ex.getMessage().contains("TableCache"), ex.getMessage());
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static List<ResolvedColumnDefinition> infer(String sql, String timeColumnName,
      Schema source) {
    return inferWithProperties(sql, Collections.singletonMap("timeColumnName", timeColumnName),
        source);
  }

  private static List<ResolvedColumnDefinition> inferWithProperties(String sql,
      Map<String, String> properties, Schema source) {
    MaterializedViewInferenceInput input = new MaterializedViewInferenceInput(
        sql, null, "mv", properties, buildTableCache(source));
    return MaterializedViewSchemaInferer.infer(input);
  }

  /// Builds a [TableCache] backed by Mockito stubs that satisfy the surface that
  /// {@code PinotCatalog} consults: name canonicalization, no logical tables, schema
  /// lookup keyed on the source table name. Mocking only these methods avoids pulling
  /// pinot-core's MockRoutingManagerFactory into pinot-sql-ddl tests.
  private static TableCache buildTableCache(Schema schema) {
    String schemaName = schema.getSchemaName();
    TableCache cache = mock(TableCache.class);
    when(cache.isIgnoreCase()).thenReturn(false);
    when(cache.getActualTableName(schemaName)).thenReturn(schemaName);
    when(cache.getActualTableName(schemaName.toLowerCase())).thenReturn(schemaName);
    when(cache.getActualLogicalTableName(anyString())).thenReturn(null);
    when(cache.getSchema(schemaName)).thenReturn(schema);
    Map<String, String> tableNameMap = new HashMap<>();
    tableNameMap.put(schemaName, schemaName);
    when(cache.getTableNameMap()).thenReturn(tableNameMap);
    when(cache.getLogicalTableNameMap()).thenReturn(Collections.emptyMap());
    when(cache.getColumnNameMap(schemaName)).thenReturn(buildColumnNameMap(schema));
    return cache;
  }

  /// Pinot's catalog uses a column-name map for case-insensitive lookups even when
  /// `isIgnoreCase()` is false; supplying a verbatim identity map keeps lookups
  /// well-defined without exercising case-insensitive routing.
  private static Map<String, String> buildColumnNameMap(Schema schema) {
    Map<String, String> m = new HashMap<>();
    for (FieldSpec spec : schema.getAllFieldSpecs()) {
      m.put(spec.getName(), spec.getName());
    }
    return m;
  }
}
