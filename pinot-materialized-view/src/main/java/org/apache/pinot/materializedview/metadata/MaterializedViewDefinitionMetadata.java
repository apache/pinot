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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.utils.JsonUtils;


/// Stores the static definition of a materialized view: what it is derived from,
/// the SQL that produces it, how time columns map, and what split parameters are needed.
///
/// Persisted in ZooKeeper under `/CONFIGS/MATERIALIZED_VIEW/DEFINITION/<viewTableNameWithType>`.
/// This ZNode changes only when the materialized view is created or its definition is altered — never
/// during routine task execution or partition-state changes.
///
/// Thread-safety: instances are effectively immutable after construction.
public class MaterializedViewDefinitionMetadata {

  private static final String BASE_TABLES_KEY = "baseTables";
  private static final String DEFINED_SQL_KEY = "definedSql";
  private static final String PARTITION_EXPR_MAPS_KEY = "partitionExprMaps";
  private static final String SPLIT_SOURCE_TIME_COLUMN_KEY = "splitSourceTimeColumn";
  private static final String SPLIT_SOURCE_TIME_FORMAT_KEY = "splitSourceTimeFormat";
  private static final String SPLIT_MATERIALIZED_VIEW_TIME_COLUMN_KEY = "splitMaterializedViewTimeColumn";
  private static final String SPLIT_MATERIALIZED_VIEW_TIME_FORMAT_KEY = "splitMaterializedViewTimeFormat";
  private static final String SPLIT_BUCKET_MS_KEY = "splitBucketMs";
  private static final String STALENESS_THRESHOLD_MS_KEY = "stalenessThresholdMs";
  private static final String REWRITE_ENABLED_KEY = "rewriteEnabled";

  private static final TypeReference<List<String>> STRING_LIST_TYPE =
      new TypeReference<List<String>>() { };
  private static final TypeReference<Map<String, String>> STRING_MAP_TYPE =
      new TypeReference<Map<String, String>>() { };

  private final String _materializedViewTableNameWithType;
  private final List<String> _baseTables;
  private final String _definedSql;

  /// Maps base-table expression strings to MV column identifiers, recording how each base
  /// table time column expression is transformed into the corresponding MV time column.
  /// For example: `{"dateTimeConvert(ts,'1:MILLISECONDS:EPOCH','1:DAYS:EPOCH','1:DAYS')": "materializedViewDay"`}
  /// or for a simple pass-through: `{"ts": "ts"`}.
  private final Map<String, String> _partitionExprMaps;

  @Nullable
  private final MaterializedViewSplitSpec _splitSpec;

  /// Per-MV staleness SLO (millis).  `0` means "no SLO check".  Broker excludes the MV when
  /// `(now - watermarkMs) > stalenessThresholdMs`.
  private final long _stalenessThresholdMs;

  /// Per-MV rewrite kill switch.  `true` (default) means broker may rewrite user queries to
  /// this MV when subsumption holds.  Operators can set `false` to keep ingestion running
  /// while temporarily routing all queries to the base table (e.g. during MV migration /
  /// schema bring-up).
  private final boolean _rewriteEnabled;

  public MaterializedViewDefinitionMetadata(String viewTableNameWithType, List<String> baseTables,
      String definedSql, Map<String, String> partitionExprMaps,
      @Nullable MaterializedViewSplitSpec splitSpec) {
    this(viewTableNameWithType, baseTables, definedSql, partitionExprMaps, splitSpec, 0L, true);
  }

  public MaterializedViewDefinitionMetadata(String viewTableNameWithType, List<String> baseTables,
      String definedSql, Map<String, String> partitionExprMaps,
      @Nullable MaterializedViewSplitSpec splitSpec, long stalenessThresholdMs, boolean rewriteEnabled) {
    _materializedViewTableNameWithType = viewTableNameWithType;
    _baseTables = baseTables;
    _definedSql = definedSql;
    _partitionExprMaps = partitionExprMaps;
    _splitSpec = splitSpec;
    _stalenessThresholdMs = stalenessThresholdMs;
    _rewriteEnabled = rewriteEnabled;
  }

  public String getMaterializedViewTableNameWithType() {
    return _materializedViewTableNameWithType;
  }

  public List<String> getBaseTables() {
    return _baseTables;
  }

  public String getDefinedSql() {
    return _definedSql;
  }

  public Map<String, String> getPartitionExprMaps() {
    return _partitionExprMaps;
  }

  @Nullable
  public MaterializedViewSplitSpec getSplitSpec() {
    return _splitSpec;
  }

  public long getStalenessThresholdMs() {
    return _stalenessThresholdMs;
  }

  public boolean isRewriteEnabled() {
    return _rewriteEnabled;
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_materializedViewTableNameWithType);
    try {
      znRecord.setSimpleField(BASE_TABLES_KEY, JsonUtils.objectToString(_baseTables));
      if (_definedSql != null) {
        znRecord.setSimpleField(DEFINED_SQL_KEY, _definedSql);
      }
      if (_partitionExprMaps != null && !_partitionExprMaps.isEmpty()) {
        znRecord.setSimpleField(PARTITION_EXPR_MAPS_KEY, JsonUtils.objectToString(_partitionExprMaps));
      }
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize MaterializedViewDefinitionMetadata", e);
    }

    if (_splitSpec != null) {
      znRecord.setSimpleField(SPLIT_SOURCE_TIME_COLUMN_KEY, _splitSpec.getSourceTimeColumn());
      znRecord.setSimpleField(SPLIT_SOURCE_TIME_FORMAT_KEY, _splitSpec.getSourceTimeFormat());
      znRecord.setSimpleField(SPLIT_MATERIALIZED_VIEW_TIME_COLUMN_KEY, _splitSpec.getMaterializedViewTimeColumn());
      String viewFormat = _splitSpec.getMaterializedViewTimeFormat();
      if (viewFormat != null) {
        znRecord.setSimpleField(SPLIT_MATERIALIZED_VIEW_TIME_FORMAT_KEY, viewFormat);
      }
      znRecord.setLongField(SPLIT_BUCKET_MS_KEY, _splitSpec.getBucketMs());
    }

    if (_stalenessThresholdMs > 0) {
      znRecord.setLongField(STALENESS_THRESHOLD_MS_KEY, _stalenessThresholdMs);
    }
    // Always persist rewriteEnabled so toggling false sticks; reader defaults to true on absence
    // for backward compat with pre-V2 definitions (rewrite enabled is the safe default).
    znRecord.setBooleanField(REWRITE_ENABLED_KEY, _rewriteEnabled);

    return znRecord;
  }

  public static MaterializedViewDefinitionMetadata fromZNRecord(ZNRecord znRecord) {
    String viewTableNameWithType = znRecord.getId();
    try {
      String baseTablesJson = znRecord.getSimpleField(BASE_TABLES_KEY);
      List<String> baseTables = baseTablesJson != null
          ? JsonUtils.stringToObject(baseTablesJson, STRING_LIST_TYPE)
          : List.of();

      String definedSql = znRecord.getSimpleField(DEFINED_SQL_KEY);

      String partitionExprMapsJson = znRecord.getSimpleField(PARTITION_EXPR_MAPS_KEY);
      Map<String, String> partitionExprMaps = partitionExprMapsJson != null
          ? JsonUtils.stringToObject(partitionExprMapsJson, STRING_MAP_TYPE)
          : new HashMap<>();

      MaterializedViewSplitSpec splitSpec = null;
      String sourceTimeColumn = znRecord.getSimpleField(SPLIT_SOURCE_TIME_COLUMN_KEY);
      if (sourceTimeColumn != null) {
        String sourceTimeFormat = znRecord.getSimpleField(SPLIT_SOURCE_TIME_FORMAT_KEY);
        String viewTimeColumn = znRecord.getSimpleField(SPLIT_MATERIALIZED_VIEW_TIME_COLUMN_KEY);
        String viewTimeFormat = znRecord.getSimpleField(SPLIT_MATERIALIZED_VIEW_TIME_FORMAT_KEY);
        long bucketMs = znRecord.getLongField(SPLIT_BUCKET_MS_KEY, 0L);
        splitSpec = new MaterializedViewSplitSpec(sourceTimeColumn, sourceTimeFormat, viewTimeColumn,
            viewTimeFormat, bucketMs);
      }

      long stalenessThresholdMs = znRecord.getLongField(STALENESS_THRESHOLD_MS_KEY, 0L);
      boolean rewriteEnabled = znRecord.getBooleanField(REWRITE_ENABLED_KEY, true);

      return new MaterializedViewDefinitionMetadata(viewTableNameWithType, baseTables, definedSql,
          partitionExprMaps, splitSpec, stalenessThresholdMs, rewriteEnabled);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to deserialize MaterializedViewDefinitionMetadata from ZNRecord", e);
    }
  }

  /// Value equality used by the controller-side DDL endpoint to distinguish "idempotent retry"
  /// (same content as existing znode → proceed) from "stale orphan from a different definedSQL"
  /// (mismatch → fail 409).
  ///
  /// Coverage at this outer level: every direct field of
  /// [MaterializedViewDefinitionMetadata] is compared
  /// (`_materializedViewTableNameWithType`, `_baseTables`, `_definedSql`, `_partitionExprMaps`,
  /// `_splitSpec`, `_stalenessThresholdMs`, `_rewriteEnabled`).
  ///
  /// Caveat about the nested [MaterializedViewSplitSpec]: its own equals/hashCode currently
  /// covers only `sourceTimeColumn`, `sourceTimeFormat`, `materializedViewTimeColumn`, and
  /// `bucketMs`. `materializedViewTimeFormat` is persisted in the ZNRecord but intentionally
  /// excluded from equality today (it was added after the existing equals/hashCode and is
  /// tracked as a separate follow-up — see `MaterializedViewMetadataTest#testSplitSpecEquals
  /// AndHashCode` which pins the current contract). Two definitions that differ ONLY in
  /// `splitSpec.materializedViewTimeFormat` will therefore compare equal here. This is by
  /// design and acceptable because the controller's idempotency check (`createIfAbsent`) only
  /// uses this comparison to short-circuit a redundant write; differing time formats with
  /// otherwise identical SplitSpecs cannot occur for the same MV produced by today's compiler
  /// pipeline.
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MaterializedViewDefinitionMetadata)) {
      return false;
    }
    MaterializedViewDefinitionMetadata other = (MaterializedViewDefinitionMetadata) o;
    return _stalenessThresholdMs == other._stalenessThresholdMs
        && _rewriteEnabled == other._rewriteEnabled
        && Objects.equals(_materializedViewTableNameWithType, other._materializedViewTableNameWithType)
        && Objects.equals(_baseTables, other._baseTables)
        && Objects.equals(_definedSql, other._definedSql)
        && Objects.equals(_partitionExprMaps, other._partitionExprMaps)
        && Objects.equals(_splitSpec, other._splitSpec);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_materializedViewTableNameWithType, _baseTables, _definedSql,
        _partitionExprMaps, _splitSpec, _stalenessThresholdMs, _rewriteEnabled);
  }

  /// Specifies the time columns used to express the split boundary `watermarkMs`:
  ///
  ///   - Source (base) side: filter `sourceTimeColumn >= watermarkMs`. The base column may use
  ///       any [DateTimeFieldSpec] format — the broker converts `watermarkMs` to the source's
  ///       native unit using `sourceTimeFormat` before attaching the filter.
  ///   - MV side: filter `viewTimeColumn < watermarkMs`. The MV column is constrained to
  ///       [DataType#TIMESTAMP] (epoch millis) by [MaterializedViewAnalyzer], so the literal is
  ///       always the raw `watermarkMs` value.
  ///
  /// Thread-safety: instances are immutable.
  public static class MaterializedViewSplitSpec {
    private final String _sourceTimeColumn;
    private final String _sourceTimeFormat;
    private final String _materializedViewTimeColumn;
    private final String _materializedViewTimeFormat;
    private final long _bucketMs;

    public MaterializedViewSplitSpec(String sourceTimeColumn, String sourceTimeFormat,
        String viewTimeColumn, String viewTimeFormat, long bucketMs) {
      _sourceTimeColumn = sourceTimeColumn;
      _sourceTimeFormat = sourceTimeFormat;
      _materializedViewTimeColumn = viewTimeColumn;
      _materializedViewTimeFormat = viewTimeFormat;
      _bucketMs = bucketMs;
    }

    public String getSourceTimeColumn() {
      return _sourceTimeColumn;
    }

    public String getSourceTimeFormat() {
      return _sourceTimeFormat;
    }

    public String getMaterializedViewTimeColumn() {
      return _materializedViewTimeColumn;
    }

    public String getMaterializedViewTimeFormat() {
      return _materializedViewTimeFormat;
    }

    public long getBucketMs() {
      return _bucketMs;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof MaterializedViewSplitSpec)) {
        return false;
      }
      MaterializedViewSplitSpec other = (MaterializedViewSplitSpec) o;
      return _bucketMs == other._bucketMs
          && Objects.equals(_sourceTimeColumn, other._sourceTimeColumn)
          && Objects.equals(_sourceTimeFormat, other._sourceTimeFormat)
          && Objects.equals(_materializedViewTimeColumn, other._materializedViewTimeColumn);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_sourceTimeColumn, _sourceTimeFormat, _materializedViewTimeColumn, _bucketMs);
    }
  }
}
