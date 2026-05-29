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
package org.apache.pinot.materializedview.metadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata.MaterializedViewSplitSpec;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Pins the shared definition-metadata builder used by both the controller-side
/// `CREATE MATERIALIZED VIEW` DDL path and the minion-side scheduler cold-start. Both call
/// sites MUST produce byte-identical metadata for the same MV so the `createIfAbsent`
/// idempotency contract holds — these tests fail if the two ever diverge.
public class MaterializedViewDefinitionMetadataBuilderTest {

  private static final String VIEW_TABLE_NAME = "mv_orders_daily";
  private static final String VIEW_TABLE_WITH_TYPE = TableNameBuilder.OFFLINE.tableNameWithType(VIEW_TABLE_NAME);
  private static final String SOURCE_RAW = "orders";
  private static final String SOURCE_OFFLINE = TableNameBuilder.OFFLINE.tableNameWithType(SOURCE_RAW);
  private static final String DEFINED_SQL = "SELECT ts, city, count(*) AS cnt FROM orders GROUP BY ts, city";

  @Test
  public void testBuildHappyPath() {
    TableConfig viewConfig = mvConfigBuilder()
        .setTaskConfig(taskConfig(Map.of(
            MaterializedViewTask.DEFINED_SQL_KEY, DEFINED_SQL,
            MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d")))
        .build();
    Schema viewSchema = viewSchema();
    TableConfig sourceConfig = sourceConfigBuilder().build();
    Schema sourceSchema = sourceSchema();
    Map<String, String> partitionExprMaps = Map.of("ts", "ts");

    MaterializedViewDefinitionMetadata def = MaterializedViewDefinitionMetadataBuilder.build(
        VIEW_TABLE_WITH_TYPE, viewConfig, viewSchema, sourceConfig, sourceSchema, SOURCE_RAW,
        DEFINED_SQL, partitionExprMaps);

    assertEquals(def.getMaterializedViewTableNameWithType(), VIEW_TABLE_WITH_TYPE);
    assertEquals(def.getBaseTables(), Collections.singletonList(SOURCE_RAW),
        "baseTables must be the RAW source name — the consistency manager's reverse index "
            + "is keyed by raw name and the scheduler's `notifyMaterializedViewConsistencyManager` "
            + "extracts raw names before lookup.");
    assertEquals(def.getDefinedSql(), DEFINED_SQL);
    assertEquals(def.getPartitionExprMaps(), partitionExprMaps);
    assertEquals(def.getStalenessThresholdMs(), MaterializedViewTask.DEFAULT_STALENESS_THRESHOLD_MS,
        "Staleness defaults to the constant when the task config key is absent.");
    assertTrue(def.isRewriteEnabled(),
        "Rewrite enabled defaults to true; toggling false is opt-in per MV.");

    MaterializedViewSplitSpec splitSpec = def.getSplitSpec();
    assertNotNull(splitSpec, "Split spec must be populated for any time-windowed MV.");
    assertEquals(splitSpec.getSourceTimeColumn(), "ts");
    // DateTimeFieldSpec normalises TIMESTAMP format down to the canonical "TIMESTAMP" tag.
    // Whatever the source schema actually round-trips through getFormat() is what the
    // builder MUST persist — pinning the live value here means a refactor to the source's
    // format handling would surface here rather than as a silent broker-rewrite divergence.
    assertEquals(splitSpec.getSourceTimeFormat(),
        sourceSchema.getSpecForTimeColumn("ts").getFormat(),
        "Source time format must come from the source schema's DateTimeFieldSpec, not the MV's.");
    assertEquals(splitSpec.getMaterializedViewTimeColumn(), "ts",
        "MV time column comes from viewTableConfig.validationConfig.timeColumnName.");
    assertEquals(splitSpec.getMaterializedViewTimeFormat(),
        viewSchema.getSpecForTimeColumn("ts").getFormat(),
        "MV time format must come from the MV schema's DateTimeFieldSpec, not the source's. "
            + "Base and MV time columns may use different formats (e.g. source 1:DAYS:EPOCH "
            + "vs MV 1:MILLISECONDS:TIMESTAMP); the persisted split spec must carry both so "
            + "broker-side split-query rewriting can convert window boundaries into each side's "
            + "native unit.");
    assertEquals(splitSpec.getBucketMs(), 86400000L,
        "bucketTimePeriod '1d' must convert to ms via TimeUtils.");
  }

  /// Two builder invocations against the same inputs must produce equal metadata so the
  /// controller-DDL and scheduler-cold-start call sites are interchangeable for createIfAbsent
  /// idempotency.
  @Test
  public void testBuildIsDeterministic() {
    TableConfig viewConfig = mvConfigBuilder()
        .setTaskConfig(taskConfig(Map.of(
            MaterializedViewTask.DEFINED_SQL_KEY, DEFINED_SQL,
            MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d",
            MaterializedViewTask.STALENESS_THRESHOLD_MS_KEY, "300000")))
        .build();
    Schema viewSchema = viewSchema();
    TableConfig sourceConfig = sourceConfigBuilder().build();
    Schema sourceSchema = sourceSchema();
    Map<String, String> partitionExprMaps = new HashMap<>();
    partitionExprMaps.put("ts", "ts");

    MaterializedViewDefinitionMetadata first = MaterializedViewDefinitionMetadataBuilder.build(
        VIEW_TABLE_WITH_TYPE, viewConfig, viewSchema, sourceConfig, sourceSchema, SOURCE_RAW,
        DEFINED_SQL, partitionExprMaps);
    MaterializedViewDefinitionMetadata second = MaterializedViewDefinitionMetadataBuilder.build(
        VIEW_TABLE_WITH_TYPE, viewConfig, viewSchema, sourceConfig, sourceSchema, SOURCE_RAW,
        DEFINED_SQL, new HashMap<>(partitionExprMaps));

    assertEquals(first, second,
        "Two builder invocations against equal inputs MUST produce equal metadata so "
            + "MaterializedViewDefinitionMetadataUtils#createIfAbsent's idempotency contract "
            + "holds across the controller DDL path and the scheduler cold-start path.");
    assertEquals(first.getStalenessThresholdMs(), 300000L,
        "Staleness override must round-trip from the task config string.");
  }

  /// `isMaterializedView=true` is the SPI-level identity invariant
  /// ([TableConfigUtils#validateMaterializedViewInvariants]); a TableConfig that fails this
  /// check should never reach the builder, so the precondition fail-fast is the wedge
  /// guarding against a hand-built TableConfig sneaking through a non-DDL caller.
  @Test
  public void testBuildRejectsNonMaterializedViewTableConfig() {
    TableConfig regularOffline = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(VIEW_TABLE_WITH_TYPE)
        .setTimeColumnName("ts")
        .build();
    Schema viewSchema = viewSchema();
    TableConfig sourceConfig = sourceConfigBuilder().build();
    Schema sourceSchema = sourceSchema();

    assertThrows(IllegalArgumentException.class, () ->
        MaterializedViewDefinitionMetadataBuilder.build(VIEW_TABLE_WITH_TYPE, regularOffline,
            viewSchema, sourceConfig, sourceSchema, SOURCE_RAW, DEFINED_SQL,
            Collections.emptyMap()));
  }

  /// bucketTimePeriod is hard-required by `MaterializedViewAnalyzer.validateTaskConfigs`; the
  /// builder must NOT silently fall back to a default if the key is missing — it would write
  /// inconsistent split specs across the controller and scheduler paths and silently produce
  /// wrong split queries.
  @Test
  public void testBuildRejectsMissingBucketTimePeriod() {
    TableConfig viewConfig = mvConfigBuilder()
        .setTaskConfig(taskConfig(Map.of(
            MaterializedViewTask.DEFINED_SQL_KEY, DEFINED_SQL)))
        .build();
    Schema viewSchema = viewSchema();
    TableConfig sourceConfig = sourceConfigBuilder().build();
    Schema sourceSchema = sourceSchema();

    assertThrows(IllegalStateException.class, () ->
        MaterializedViewDefinitionMetadataBuilder.build(VIEW_TABLE_WITH_TYPE, viewConfig,
            viewSchema, sourceConfig, sourceSchema, SOURCE_RAW, DEFINED_SQL,
            Collections.emptyMap()));
  }

  /// Malformed staleness in a persisted znode (i.e. one that bypassed the analyzer-side
  /// validation) must NOT throw — the builder is also called from the backfill / recovery
  /// path on already-persisted configs, where silently falling back to the default keeps
  /// the cluster running. Validation of malformed values is the analyzer's job at CREATE
  /// time; see MaterializedViewAnalyzer.validateTaskConfigs.
  @Test
  public void testBuildFallsBackOnMalformedStaleness() {
    TableConfig viewConfig = mvConfigBuilder()
        .setTaskConfig(taskConfig(Map.of(
            MaterializedViewTask.DEFINED_SQL_KEY, DEFINED_SQL,
            MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d",
            MaterializedViewTask.STALENESS_THRESHOLD_MS_KEY, "not-a-number")))
        .build();
    Schema viewSchema = viewSchema();
    TableConfig sourceConfig = sourceConfigBuilder().build();
    Schema sourceSchema = sourceSchema();

    MaterializedViewDefinitionMetadata def = MaterializedViewDefinitionMetadataBuilder.build(
        VIEW_TABLE_WITH_TYPE, viewConfig, viewSchema, sourceConfig, sourceSchema, SOURCE_RAW,
        DEFINED_SQL, Collections.emptyMap());
    assertEquals(def.getStalenessThresholdMs(), MaterializedViewTask.DEFAULT_STALENESS_THRESHOLD_MS);
  }

  // ── helpers ──

  private static TableConfigBuilder mvConfigBuilder() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(VIEW_TABLE_WITH_TYPE)
        .setIsMaterializedView(true)
        .setTimeColumnName("ts");
  }

  private static TableConfigBuilder sourceConfigBuilder() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(SOURCE_OFFLINE)
        .setTimeColumnName("ts");
  }

  private static TableTaskConfig taskConfig(Map<String, String> configs) {
    return new TableTaskConfig(Map.of(MaterializedViewTask.TASK_TYPE, configs));
  }

  private static Schema sourceSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(SOURCE_RAW)
        .addDateTime("ts", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .addSingleValueDimension("city", DataType.STRING)
        .build();
  }

  /// MV-side schema. Deliberately uses a DIFFERENT `DateTimeFieldSpec` format than the source
  /// schema so the happy-path test can independently assert that source-side and MV-side time
  /// formats end up on the right field of [MaterializedViewSplitSpec]. A wiring swap in the
  /// builder would surface here as the source format leaking into the MV slot or vice versa.
  /// The choice of `1:DAYS:EPOCH` mirrors a realistic MV that buckets a TIMESTAMP source into
  /// daily granularity (e.g. `DATETRUNC('DAY', ts)` or `ts / 86400000`).
  private static Schema viewSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(VIEW_TABLE_NAME)
        .addDateTime("ts", DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .addSingleValueDimension("city", DataType.STRING)
        .build();
  }
}
