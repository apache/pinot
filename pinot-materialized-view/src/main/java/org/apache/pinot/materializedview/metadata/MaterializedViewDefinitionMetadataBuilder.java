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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata.MaterializedViewSplitSpec;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.pinot.spi.utils.TimeUtils;


/// Single source of truth for constructing a [MaterializedViewDefinitionMetadata] from a
/// fully-validated `(viewTableConfig, viewSchema, sourceTableConfig, sourceSchema)` quadruple
/// plus the `definedSQL` and analyzer-derived `partitionExprMaps`.
///
/// Two call sites use this builder, and they MUST produce byte-identical metadata for the
/// same MV so the "create-if-absent" idempotence in
/// [MaterializedViewDefinitionMetadataUtils#createIfAbsent] is safe across both paths:
///
///   1. The controller-side `CREATE MATERIALIZED VIEW` DDL endpoint persists the znode
///      immediately so `notifyMaterializedViewConsistencyManagerForTableCreate` finds it
///      during the same `addTable` call and registers the MV with its authoritative
///      `baseTables` list (the fallback path is fragile under any future relaxation of the
///      analyzer's single-FROM contract).
///   2. The minion-side `MaterializedViewTaskScheduler` lazy-initialises the znode on
///      cold-start when the controller-side write did not happen — e.g. an MV created before
///      this builder existed, or an MV whose CREATE failed midway and was recovered.
///
/// This class does NOT re-run MV validation: every caller MUST have already invoked
/// [org.apache.pinot.materializedview.analysis.MaterializedViewAnalyzer#analyze] (directly
/// or transitively via `TaskConfigUtils.validateTaskConfigs`) against the same inputs.
/// Preconditions here are tight invariants that a successful `analyze()` guarantees; failing
/// them indicates a bug or hand-edited cluster state, not a user-facing error.
public final class MaterializedViewDefinitionMetadataBuilder {

  private MaterializedViewDefinitionMetadataBuilder() {
  }

  /// Builds the definition metadata for an MV.
  ///
  /// @param viewTableNameWithType  the MV's fully-qualified name (must end with `_OFFLINE`)
  /// @param viewTableConfig        the MV's [TableConfig] (must have
  ///                               `isMaterializedView=true` and a configured
  ///                               `MaterializedViewTask` task config with `bucketTimePeriod`)
  /// @param viewSchema             the MV's [Schema] (must declare a [DateTimeFieldSpec] for
  ///                               the MV's time column). Required so the persisted split spec
  ///                               carries the MV-side `getFormat()` value alongside the
  ///                               source-side one — base and MV time columns may use
  ///                               different `DateTimeFieldSpec` formats, and broker-side
  ///                               split-query rewriting needs both to convert window
  ///                               boundaries into each side's native unit.
  /// @param sourceTableConfig      the source/base table's [TableConfig] (must have
  ///                               `validationConfig.timeColumnName` set)
  /// @param sourceSchema           the source/base table's [Schema] (must declare a
  ///                               [DateTimeFieldSpec] for the source's time column)
  /// @param sourceRawTableName     the raw source table name as it appears in the MV's
  ///                               `FROM` clause. This is the key the consistency manager
  ///                               indexes by (it strips the `_OFFLINE`/`_REALTIME` suffix
  ///                               internally), so we persist the raw name here even when the
  ///                               resolved source happens to be a hybrid pair — the
  ///                               consistency manager reverse-index entry is per raw name.
  /// @param definedSql             the MV's `AS <query>` body, exactly as persisted under
  ///                               `task.MaterializedViewTask.definedSQL`
  /// @param partitionExprMaps      analyzer-derived `exprString -> mvColumn` map
  public static MaterializedViewDefinitionMetadata build(String viewTableNameWithType,
      TableConfig viewTableConfig, Schema viewSchema, TableConfig sourceTableConfig,
      Schema sourceSchema, String sourceRawTableName, String definedSql,
      Map<String, String> partitionExprMaps) {
    Preconditions.checkArgument(viewTableNameWithType != null && !viewTableNameWithType.isEmpty(),
        "viewTableNameWithType must be set");
    Preconditions.checkArgument(viewTableConfig != null, "viewTableConfig must be set");
    Preconditions.checkArgument(viewTableConfig.isMaterializedView(),
        "viewTableConfig.isMaterializedView must be true for: %s", viewTableNameWithType);
    Preconditions.checkArgument(viewSchema != null, "viewSchema must be set");
    Preconditions.checkArgument(sourceTableConfig != null, "sourceTableConfig must be set");
    Preconditions.checkArgument(sourceSchema != null, "sourceSchema must be set");
    Preconditions.checkArgument(sourceRawTableName != null && !sourceRawTableName.isEmpty(),
        "sourceRawTableName must be set");
    Preconditions.checkArgument(definedSql != null && !definedSql.isEmpty(),
        "definedSql must be set");
    Preconditions.checkArgument(partitionExprMaps != null, "partitionExprMaps must be set");

    TableTaskConfig viewTaskConfig = viewTableConfig.getTaskConfig();
    Preconditions.checkState(viewTaskConfig != null,
        "MV table %s must have a TableTaskConfig", viewTableNameWithType);
    Map<String, String> mvTaskConfigs =
        viewTaskConfig.getConfigsForTaskType(MaterializedViewTask.TASK_TYPE);
    Preconditions.checkState(mvTaskConfigs != null,
        "MV table %s must have MaterializedViewTask configs", viewTableNameWithType);

    // Bucket — required. `validateMaterializedViewConsistency` (compile-side) and
    // `MaterializedViewAnalyzer.validateTaskConfigs` (controller-side) both enforce this is
    // present and parseable before we get here; re-checking returns the same Pinot period
    // representation so the value reaches the znode unchanged.
    String bucketTimePeriod = mvTaskConfigs.get(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY);
    Preconditions.checkState(bucketTimePeriod != null && !bucketTimePeriod.isEmpty(),
        "MV table %s missing %s in MaterializedViewTask configs",
        viewTableNameWithType, MaterializedViewTask.BUCKET_TIME_PERIOD_KEY);
    long bucketMs = TimeUtils.convertPeriodToMillis(bucketTimePeriod);
    Preconditions.checkState(bucketMs > 0,
        "MV table %s bucketTimePeriod must produce positive ms, got: %s",
        viewTableNameWithType, bucketTimePeriod);

    // Staleness — optional, defaults to the constant. Validation happens up-front in
    // `MaterializedViewAnalyzer.validateTaskConfigs`; this fallback exists for the
    // backfill/recovery path that runs on already-persisted znodes (which may predate the
    // analyzer-side check), so silent fallback is the right behavior here.
    long stalenessThresholdMs = parseLongOrDefault(
        mvTaskConfigs.get(MaterializedViewTask.STALENESS_THRESHOLD_MS_KEY),
        MaterializedViewTask.DEFAULT_STALENESS_THRESHOLD_MS);

    // Source side: time column + format. `MaterializedViewAnalyzer.validateSourceTable`
    // guarantees the source has a non-empty `timeColumnName` and a `DateTimeFieldSpec`.
    String sourceTimeColumn = sourceTableConfig.getValidationConfig().getTimeColumnName();
    Preconditions.checkState(sourceTimeColumn != null && !sourceTimeColumn.isEmpty(),
        "Source table %s missing timeColumnName (MV: %s)",
        sourceTableConfig.getTableName(), viewTableNameWithType);
    DateTimeFieldSpec sourceTimeFieldSpec = sourceSchema.getSpecForTimeColumn(sourceTimeColumn);
    Preconditions.checkState(sourceTimeFieldSpec != null,
        "Source table %s has no DateTimeFieldSpec for time column '%s' (MV: %s)",
        sourceTableConfig.getTableName(), sourceTimeColumn, viewTableNameWithType);

    // MV side: time column + format. `MaterializedViewAnalyzer.validateMaterializedViewTimeColumnAlignment`
    // guarantees the column is set, exists in the MV schema as a DateTimeFieldSpec, and is
    // produced by the SELECT. We resolve the format here from the MV schema (mirroring the
    // scheduler's `resolveMaterializedViewTimeFormat`) so the persisted split spec carries
    // both source and MV `getFormat()` values — broker-side split-query rewriting needs each
    // side independently because base and MV time columns may use different formats.
    String viewTimeColumn = viewTableConfig.getValidationConfig().getTimeColumnName();
    Preconditions.checkState(viewTimeColumn != null && !viewTimeColumn.isEmpty(),
        "MV table %s missing timeColumnName", viewTableNameWithType);
    DateTimeFieldSpec viewTimeFieldSpec = viewSchema.getSpecForTimeColumn(viewTimeColumn);
    Preconditions.checkState(viewTimeFieldSpec != null,
        "MV table %s has no DateTimeFieldSpec for time column '%s'",
        viewTableNameWithType, viewTimeColumn);

    MaterializedViewSplitSpec splitSpec = new MaterializedViewSplitSpec(
        sourceTimeColumn, sourceTimeFieldSpec.getFormat(),
        viewTimeColumn, viewTimeFieldSpec.getFormat(),
        bucketMs);

    return new MaterializedViewDefinitionMetadata(
        viewTableNameWithType,
        List.of(sourceRawTableName),
        definedSql,
        partitionExprMaps,
        splitSpec,
        stalenessThresholdMs,
        /*rewriteEnabled=*/ true);
  }

  private static long parseLongOrDefault(String value, long defaultValue) {
    if (value == null || value.isEmpty()) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }
}
