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
package org.apache.pinot.segment.local.utils;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Migration helper that converts legacy [FieldConfig.CompressionCodec] settings to their
/// recommended codec pipeline `codecSpec` strings for the forward-index codec framework.
///
/// ### Mapping table
/// ```
///   PASS_THROUGH  → not migrated (raw no-op, no codec pipeline needed)
///   LZ4           → "LZ4"
///   ZSTANDARD     → "ZSTD(3)"
///   SNAPPY        → "SNAPPY"
///   GZIP          → "GZIP"
///   DELTA         → "CODEC(DELTA,LZ4)"      (NOTE: adds LZ4 byte compression; prior DELTA stored
///                                             delta-encoded values without any byte-level compression)
///   DELTADELTA    → "CODEC(DELTADELTA,LZ4)" (same NOTE as DELTA: adds LZ4 byte compression)
///   MV_ENTRY_DICT → not migrated (dict-encoded MV index, not a raw codec)
///   CLP / CLPV2 / CLPV2_ZSTD / CLPV2_LZ4 → not migrated (CLP has its own pipeline)
/// ```
///
/// Compression-only codec specs use the existing raw forward-index writers and are supported
/// for all raw forward-index column shapes that already support the corresponding legacy
/// compression codec. Transform specs (`DELTA` / `DELTADELTA`) use the V7 codec-pipeline
/// writer and remain restricted to single-value INT/LONG RAW columns.
///
/// ### Usage
///
/// Call [#migrateTableConfig(TableConfig)] to produce a new [TableConfig] where
/// every migratable [FieldConfig] has its `compressionCodec` replaced by the
/// recommended `codecSpec`.  The original [TableConfig] is not modified.
///
/// This helper is intended for use in tooling and admin APIs (e.g. a minion task or a
/// controller endpoint) that performs transparent migration after upgrading to 1.6.0.
/// During migration both config paths remain supported so that existing segments and configs can
/// be rolled forward gradually.
///
/// **Concurrency:** the methods on this helper are pure functions over the input config —
/// they do not touch ZooKeeper. Callers that persist the migrated config *must* use a
/// version-checked write (Helix CAS / ZK optimistic locking) to avoid clobbering concurrent admin
/// updates that may have landed between the read-and-migrate and the write.
///
/// TODO: Remove this class in 2.0 once all deployments have completed migration from
/// `compressionCodec` to `codecSpec` and the legacy `compressionCodec`
/// field is removed from [FieldConfig].
// TODO: Remove in 2.0 (see class Javadoc)
public final class CompressionCodecMigrator {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompressionCodecMigrator.class);

  /// JSON key for the forward-index block under `FieldConfig.indexes` (matches
  /// `ForwardIndexType.INDEX_DISPLAY_NAME`). codecSpec is configured only under this block.
  private static final String FORWARD_INDEX_KEY = "forward";

  private CompressionCodecMigrator() {
  }

  /// Returns the codec pipeline `codecSpec` string equivalent to `codec`, or
  /// `null` if the codec cannot be expressed in the pipeline framework (e.g. CLP,
  /// MV_ENTRY_DICT).
  ///
  /// @param codec the legacy compression codec; `null` is treated as "no codec" and returns
  ///              `null`
  @Nullable
  public static String toCodecSpec(@Nullable FieldConfig.CompressionCodec codec) {
    if (codec == null) {
      return null;
    }
    switch (codec) {
      case LZ4:
        return "LZ4";
      case ZSTANDARD:
        return "ZSTD(3)";
      case SNAPPY:
        return "SNAPPY";
      case GZIP:
        return "GZIP";
      case DELTA:
        return "CODEC(DELTA,LZ4)";
      case DELTADELTA:
        return "CODEC(DELTADELTA,LZ4)";
      default:
        // PASS_THROUGH: no-op codec, no migration target
        // MV_ENTRY_DICT, CLP, CLPV2, CLPV2_ZSTD, CLPV2_LZ4: not supported in codec pipeline
        return null;
    }
  }

  /// Returns `true` if `codec` can be transparently migrated to a codec pipeline spec.
  public static boolean isMigratable(@Nullable FieldConfig.CompressionCodec codec) {
    return toCodecSpec(codec) != null;
  }

  /// Returns `true` if the given column can be safely migrated to a codec pipeline spec.
  /// Unlike [#isMigratable(FieldConfig.CompressionCodec)], this overload also verifies that
  /// transform codecs are only migrated on single-value INT or LONG columns.
  ///
  /// @param fieldConfig the field configuration to evaluate
  /// @param schema      the table schema; if `null` the column-type check is skipped (legacy
  ///                    callers that have no schema available)
  public static boolean isMigratableWithSchema(FieldConfig fieldConfig, @Nullable Schema schema) {
    FieldConfig.CompressionCodec codec = fieldConfig.getCompressionCodec();
    if (!isMigratable(codec)) {
      return false;
    }
    if (schema == null) {
      return true;
    }
    FieldSpec fieldSpec = schema.getFieldSpecFor(fieldConfig.getName());
    if (fieldSpec == null) {
      return false;
    }
    if (!isTransformCodec(codec)) {
      return true;
    }
    if (!fieldSpec.isSingleValueField()) {
      return false;
    }
    FieldSpec.DataType stored = fieldSpec.getDataType().getStoredType();
    return stored == FieldSpec.DataType.INT || stored == FieldSpec.DataType.LONG;
  }

  /// Returns a new [FieldConfig] where the `compressionCodec` has been replaced by its
  /// equivalent `codecSpec`, or the original instance if the codec is not migratable.
  ///
  /// This overload is column-type-agnostic. Use [#migrate(FieldConfig, Schema)] when a
  /// schema is available to avoid migrating transform codecs on unsupported column types.
  public static FieldConfig migrate(FieldConfig fieldConfig) {
    FieldConfig.CompressionCodec codec = fieldConfig.getCompressionCodec();
    String codecSpec = toCodecSpec(codec);
    if (codecSpec == null) {
      return fieldConfig;
    }
    if (codec == FieldConfig.CompressionCodec.DELTA || codec == FieldConfig.CompressionCodec.DELTADELTA) {
      // Migrating DELTA/DELTADELTA to CODEC(...,LZ4) adds byte-level compression that the legacy
      // path did not have. Existing segments will be rewritten to V7 on next reload — log so
      // operators can plan for the rewrite cost.
      LOGGER.warn("Migrating column '{}' from {} to '{}' adds LZ4 byte compression. Existing segments will be"
              + " rewritten to V7 on next reload.", fieldConfig.getName(), codec, codecSpec);
    }
    // codecSpec is configured only via the modern `indexes.forward` block (no top-level FieldConfig
    // field). Inject it there and drop the legacy top-level compressionCodec.
    ObjectNode indexes = fieldConfig.getIndexes() instanceof ObjectNode
        ? (ObjectNode) fieldConfig.getIndexes().deepCopy() : JsonUtils.newObjectNode();
    ObjectNode forwardNode = indexes.get(FORWARD_INDEX_KEY) instanceof ObjectNode
        ? (ObjectNode) indexes.get(FORWARD_INDEX_KEY) : JsonUtils.newObjectNode();
    // Drop any pre-existing inner compressionCodec: it is mutually exclusive with codecSpec and
    // would otherwise trip ForwardIndexConfig's mutual-exclusion guard when the migrated config is
    // deserialized on reload.
    forwardNode.remove("compressionCodec");
    forwardNode.put("codecSpec", codecSpec);
    indexes.set(FORWARD_INDEX_KEY, forwardNode);
    return new FieldConfig.Builder(fieldConfig)
        .withCompressionCodec(null)
        .withIndexes(indexes)
        .build();
  }

  /// Returns a new [FieldConfig] where the `compressionCodec` has been replaced by its
  /// equivalent `codecSpec`, or the original instance if the codec is not migratable for the
  /// given column's stored type.
  ///
  /// When `schema` is provided the migration is skipped for transform codecs on columns whose
  /// stored type is not single-value INT or LONG.
  ///
  /// @param fieldConfig the field configuration to migrate
  /// @param schema      the table schema; if `null` falls back to type-agnostic migration
  public static FieldConfig migrate(FieldConfig fieldConfig, @Nullable Schema schema) {
    if (!isMigratableWithSchema(fieldConfig, schema)) {
      return fieldConfig;
    }
    return migrate(fieldConfig);
  }

  /// Returns a new [TableConfig] where every migratable [FieldConfig] in the field
  /// config list has its `compressionCodec` replaced by the equivalent `codecSpec`.
  /// The original [TableConfig] is not modified.
  ///
  /// Uses schema-aware migration: transform codecs are left on the legacy `compressionCodec`
  /// path unless the column is single-value INT or LONG.
  ///
  /// @param tableConfig the table configuration to migrate
  /// @param schema      the table schema used for column-type checks; `null` falls back to
  ///                    type-agnostic migration (legacy behaviour)
  public static TableConfig migrateTableConfig(TableConfig tableConfig, @Nullable Schema schema) {
    List<FieldConfig> original = tableConfig.getFieldConfigList();
    if (original == null || original.isEmpty()) {
      return tableConfig;
    }

    boolean anyMigrated = false;
    List<FieldConfig> migrated = new ArrayList<>(original.size());
    for (FieldConfig fc : original) {
      FieldConfig updated = migrate(fc, schema);
      migrated.add(updated);
      if (updated != fc) {
        anyMigrated = true;
      }
    }

    if (!anyMigrated) {
      return tableConfig;
    }

    TableConfig copy = new TableConfig(tableConfig);
    copy.setFieldConfigList(migrated);
    return copy;
  }

  /// Returns a new [TableConfig] where every migratable [FieldConfig] in the field
  /// config list has its `compressionCodec` replaced by the equivalent `codecSpec`.
  /// The original [TableConfig] is not modified.
  ///
  /// This overload is column-type-agnostic. Prefer [#migrateTableConfig(TableConfig, Schema)]
  /// when a schema is available.
  ///
  /// If no field configs are migratable, the original [TableConfig] is returned unchanged.
  public static TableConfig migrateTableConfig(TableConfig tableConfig) {
    List<FieldConfig> original = tableConfig.getFieldConfigList();
    if (original == null || original.isEmpty()) {
      return tableConfig;
    }

    boolean anyMigrated = false;
    List<FieldConfig> migrated = new ArrayList<>(original.size());
    for (FieldConfig fc : original) {
      FieldConfig updated = migrate(fc);
      migrated.add(updated);
      if (updated != fc) {
        anyMigrated = true;
      }
    }

    if (!anyMigrated) {
      return tableConfig;
    }

    TableConfig copy = new TableConfig(tableConfig);
    copy.setFieldConfigList(migrated);
    return copy;
  }

  private static boolean isTransformCodec(FieldConfig.CompressionCodec codec) {
    return codec == FieldConfig.CompressionCodec.DELTA || codec == FieldConfig.CompressionCodec.DELTADELTA;
  }
}
