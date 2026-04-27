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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.table.CompletionConfig;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.DimensionTableConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.TunerConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.SegmentAssignmentConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.sampler.TableSamplerConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.ddl.resolved.ResolvedTableDefinition;


/// Routes Pinot DDL `PROPERTIES (...)` entries onto a [TableConfigBuilder].
///
/// Routing rules (applied in order):
/// 1. If the key (case-insensitive) is in the promoted catalog, set the corresponding
/// [TableConfigBuilder] field directly.
/// 1. If the key is `streamType` or starts with `stream.` or `realtime.`, route the entry into
/// `IndexingConfig.streamConfigs` verbatim (the prefix is preserved, not stripped, so
/// the key matches existing Pinot stream config conventions like `streamType` and
/// `stream.kafka.topic.name`). REALTIME-only.
/// 1. If the key starts with `task.<taskType>.`, route the remainder into
/// `TableTaskConfig.taskTypeConfigsMap[taskType]`.
/// 1. Otherwise, store verbatim in [TableCustomConfig#getCustomConfigs()].
/// This guarantees no silent loss of meaningful config: every DDL property survives the
/// compile / persist round-trip, even when the DDL grammar is older than the property.
///
/// The free-form pass-through (rules 2-4) is the forward-compatibility hook: stream and minion
/// task config schemas evolve independently and need not be in lock-step with the DDL grammar.
public final class PropertyMapping {

  private static final String STREAM_PREFIX = "stream.";
  private static final String STREAM_TYPE_PROPERTY = "streamType";
  private static final String STREAM_TYPE_KEY = "streamtype";
  private static final String REALTIME_PREFIX = "realtime.";
  private static final String TASK_PREFIX = "task.";

  /// Property keys that carry table-type semantics, handled separately by the caller.
  static final Set<String> RESERVED_KEYS = ImmutableSet.of("tabletype", "tablename", "ifnotexists");

  /// Lowercase property keys that have a dedicated PropertyMapping handler (promoted scalar or
  /// JSON-blob deserialization). Used by the reverse compiler to detect TableCustomConfig keys
  /// that would shadow a reserved key on round-trip and reject them up front.
  private static final Set<String> RESERVED_ROUND_TRIP_KEYS;
  static {
    Set<String> keys = new HashSet<>();
    keys.add("timecolumnname");
    keys.add("timecolumn");
    keys.add("timetype");
    keys.add("replication");
    keys.add("retentiontimeunit");
    keys.add("retentiontimevalue");
    keys.add("brokertenant");
    keys.add("servertenant");
    keys.add("loadmode");
    keys.add("sortedcolumn");
    keys.add("nullhandlingenabled");
    keys.add("isdimtable");
    keys.add("invertedindexcolumns");
    keys.add("nodictionarycolumns");
    keys.add("bloomfiltercolumns");
    keys.add("rangeindexcolumns");
    keys.add("jsonindexcolumns");
    keys.add("varlengthdictionarycolumns");
    keys.add("onheapdictionarycolumns");
    keys.add("peersegmentdownloadscheme");
    keys.add("crypterclassname");
    keys.add("deletedsegmentsretentionperiod");
    keys.add("segmentversion");
    keys.add("aggregatemetrics");
    keys.add("description");
    keys.add("tags");
    keys.add("replicasperpartition");
    keys.add("ingestionconfig");
    keys.add("upsertconfig");
    keys.add("dedupconfig");
    keys.add("dimensiontableconfig");
    keys.add("routingconfig");
    keys.add("queryconfig");
    keys.add("quotaconfig");
    keys.add("tierconfigs");
    keys.add("tunerconfigs");
    keys.add("fieldconfigs");
    keys.add("instanceassignmentconfigmap");
    keys.add("tagoverrideconfig");
    keys.add("replicagroupstrategyconfig");
    keys.add("completionconfig");
    keys.add("startreeindexconfigs");
    keys.add("segmentpartitionconfig");
    keys.add("multicolumntextindexconfig");
    keys.add("jsonindexconfigs");
    keys.add("instancepartitionsmap");
    keys.add("segmentassignmentconfigmap");
    keys.add("tablesamplers");
    keys.add("tieroverwrites");
    keys.addAll(RESERVED_KEYS);
    RESERVED_ROUND_TRIP_KEYS = Collections.unmodifiableSet(keys);
  }

  /// Returns `true` if `lowerKey` (already lower-cased) is consumed by a dedicated
  /// PropertyMapping handler — i.e. it would not round-trip safely as a TableCustomConfig entry.
  ///
  /// Catches both exact-match keys (promoted scalars + JSON-blob keys) and the
  /// prefix-routed paths (`streamType`, `stream.`, `realtime.`, `task.`). A custom-config
  /// entry with a key like `task.MinionTask.foo` would otherwise be silently routed into
  /// `TableTaskConfig` on re-parse.
  public static boolean isReservedRoundTripKey(String lowerKey) {
    if (RESERVED_ROUND_TRIP_KEYS.contains(lowerKey)) {
      return true;
    }
    return STREAM_TYPE_KEY.equals(lowerKey)
        || lowerKey.startsWith(STREAM_PREFIX)
        || lowerKey.startsWith(REALTIME_PREFIX)
        || lowerKey.startsWith(TASK_PREFIX);
  }

  private PropertyMapping() {
  }

  /// Applies all properties from `definition` onto `builder`.
  ///
  /// Returns the full list of sorted columns parsed from the `sortedColumn` property so
  /// the caller can apply them directly to [IndexingConfig#setSortedColumn(List)] after
  /// `builder.build()`. The builder's `setSortedColumn(String)` only stores a single
  /// value; using the returned list avoids silently dropping all but the first sort column.
  ///
  /// @throws DdlCompilationException if a promoted key has a non-coercible value (e.g. non-integer
  /// replication).
  public static List<String> apply(ResolvedTableDefinition definition, TableConfigBuilder builder) {
    Map<String, String> streamConfigs = new LinkedHashMap<>();
    Map<String, Map<String, String>> taskConfigs = new LinkedHashMap<>();
    Map<String, String> customConfigs = new LinkedHashMap<>();
    List<String> sortedColumns = null;

    for (Map.Entry<String, String> entry : definition.getProperties().entrySet()) {
      String rawKey = entry.getKey();
      String value = entry.getValue();
      String lower = rawKey.toLowerCase(Locale.ROOT);

      if (RESERVED_KEYS.contains(lower)) {
        // Reserved keys are handled by the caller (e.g. tableType from the TABLE_TYPE clause).
        // If they appear in PROPERTIES we treat that as a conflicting clause.
        throw new DdlCompilationException(
            "Property '" + rawKey + "' is reserved and must be expressed via a first-class clause, "
                + "not PROPERTIES.");
      }

      if (lower.equals("sortedcolumn")) {
        // Capture the full list so the caller can call IndexingConfig.setSortedColumn(List)
        // after build(). Also set the first element via the builder so single-column sort
        // configs still work when the caller ignores the returned list.
        sortedColumns = splitCsv(value);
        if (!sortedColumns.isEmpty()) {
          builder.setSortedColumn(sortedColumns.get(0));
        }
        continue;
      }

      if (applyPromoted(lower, value, builder)) {
        continue;
      }

      // JSON-blob property keys: complex nested configs (ingestionConfig, upsertConfig, etc.)
      // round-trip via PROPERTIES('<key>' = '<json>'). The reverse compiler emits these the
      // same way so canonical DDL preserves all meaningful TableConfig state.
      if (applyJsonBlob(rawKey, lower, value, builder)) {
        continue;
      }

      if (STREAM_TYPE_KEY.equals(lower) || lower.startsWith(STREAM_PREFIX)
          || lower.startsWith(REALTIME_PREFIX)) {
        // "streamType", "stream.*" (Pinot stream connection configs), and "realtime.*" (e.g.
        // realtime.segment.flush.threshold.rows / .size / .segment.size) live in
        // IndexingConfig.streamConfigs in real Pinot table configs. Routing them anywhere else
        // makes them silently inert.
        if (definition.getTableType() != TableType.REALTIME) {
          String kind = lower.startsWith(REALTIME_PREFIX) ? "Realtime" : "Stream";
          throw new DdlCompilationException(
              kind + " property '" + rawKey + "' is only valid for REALTIME tables.");
        }
        // Preserve ordinary stream keys verbatim so existing Pinot stream configs round-trip
        // identically. Canonicalize the special un-prefixed streamType key because Pinot's stream
        // config readers use that exact casing.
        streamConfigs.put(STREAM_TYPE_KEY.equals(lower) ? STREAM_TYPE_PROPERTY : rawKey, value);
        continue;
      }

      if (lower.startsWith(TASK_PREFIX)) {
        // task.<taskType>.<key> = value
        String afterPrefix = rawKey.substring(TASK_PREFIX.length());
        int dot = afterPrefix.indexOf('.');
        if (dot <= 0 || dot == afterPrefix.length() - 1) {
          throw new DdlCompilationException(
              "Task property '" + rawKey + "' must follow the form task.<taskType>.<key>");
        }
        String taskType = afterPrefix.substring(0, dot);
        String taskKey = afterPrefix.substring(dot + 1);
        taskConfigs.computeIfAbsent(taskType, k -> new LinkedHashMap<>()).put(taskKey, value);
        continue;
      }

      customConfigs.put(rawKey, value);
    }

    if (!streamConfigs.isEmpty()) {
      builder.setStreamConfigs(streamConfigs);
    }
    if (!taskConfigs.isEmpty()) {
      builder.setTaskConfig(new TableTaskConfig(taskConfigs));
    }
    if (!customConfigs.isEmpty()) {
      builder.setCustomConfig(new TableCustomConfig(customConfigs));
    }
    return sortedColumns;
  }

  /// Returns true if `lowerKey` matched a promoted property and was applied.
  private static boolean applyPromoted(String lowerKey, String value, TableConfigBuilder builder) {
    switch (lowerKey) {
      case "timecolumnname":
      case "timecolumn":
        builder.setTimeColumnName(value);
        return true;
      case "timetype":
        builder.setTimeType(value);
        return true;
      case "replication":
        builder.setNumReplicas(parseInt(lowerKey, value));
        return true;
      case "retentiontimeunit":
        builder.setRetentionTimeUnit(value);
        return true;
      case "retentiontimevalue":
        builder.setRetentionTimeValue(value);
        return true;
      case "brokertenant":
        builder.setBrokerTenant(value);
        return true;
      case "servertenant":
        builder.setServerTenant(value);
        return true;
      case "loadmode":
        builder.setLoadMode(value);
        return true;
      case "sortedcolumn":
        // Handled above the applyPromoted call so the full column list is captured.
        // This branch is dead code but kept to satisfy the switch exhaustiveness check.
        return true;
      case "nullhandlingenabled":
        builder.setNullHandlingEnabled(parseBool(lowerKey, value));
        return true;
      case "isdimtable":
        builder.setIsDimTable(parseBool(lowerKey, value));
        return true;
      case "invertedindexcolumns":
        builder.setInvertedIndexColumns(splitCsv(value));
        return true;
      case "nodictionarycolumns":
        builder.setNoDictionaryColumns(splitCsv(value));
        return true;
      case "bloomfiltercolumns":
        builder.setBloomFilterColumns(splitCsv(value));
        return true;
      case "rangeindexcolumns":
        builder.setRangeIndexColumns(splitCsv(value));
        return true;
      case "jsonindexcolumns":
        builder.setJsonIndexColumns(splitCsv(value));
        return true;
      case "varlengthdictionarycolumns":
        builder.setVarLengthDictionaryColumns(splitCsv(value));
        return true;
      case "onheapdictionarycolumns":
        builder.setOnHeapDictionaryColumns(splitCsv(value));
        return true;
      case "peersegmentdownloadscheme":
        builder.setPeerSegmentDownloadScheme(value);
        return true;
      case "crypterclassname":
        builder.setCrypterClassName(value);
        return true;
      case "deletedsegmentsretentionperiod":
        builder.setDeletedSegmentsRetentionPeriod(value);
        return true;
      case "segmentversion":
        builder.setSegmentVersion(value);
        return true;
      case "aggregatemetrics":
        builder.setAggregateMetrics(parseBool(lowerKey, value));
        return true;
      case "description":
        builder.setDescription(value);
        return true;
      case "tags":
        builder.setTags(splitCsv(value));
        return true;
      case "replicasperpartition":
        // TableConfigBuilder does not expose setReplicasPerPartition; DdlCompiler applies this
        // value post-build via tableConfig.getValidationConfig().setReplicasPerPartition().
        // Validate as an integer here to fail fast at compile time with a clear error rather
        // than letting a non-numeric value land in TableConfig and surface only at validation.
        // Mirrors the eager parseInt validation done for "replication" above.
        parseInt(lowerKey, value);
        return true;
      default:
        return false;
    }
  }

  /// Recognizes JSON-blob property keys for complex nested TableConfig fields and deserializes
  /// them back into the right setter on [TableConfigBuilder]. Returns true when the key
  /// was handled (regardless of value validity — invalid JSON throws so the user sees a 400).
  private static boolean applyJsonBlob(String rawKey, String lowerKey, String value,
      TableConfigBuilder builder) {
    try {
      switch (lowerKey) {
        case "ingestionconfig":
          builder.setIngestionConfig(JsonUtils.stringToObject(value, IngestionConfig.class));
          return true;
        case "upsertconfig":
          builder.setUpsertConfig(JsonUtils.stringToObject(value, UpsertConfig.class));
          return true;
        case "dedupconfig":
          builder.setDedupConfig(JsonUtils.stringToObject(value, DedupConfig.class));
          return true;
        case "dimensiontableconfig":
          builder.setDimensionTableConfig(
              JsonUtils.stringToObject(value, DimensionTableConfig.class));
          return true;
        case "routingconfig":
          builder.setRoutingConfig(JsonUtils.stringToObject(value, RoutingConfig.class));
          return true;
        case "queryconfig":
          builder.setQueryConfig(JsonUtils.stringToObject(value, QueryConfig.class));
          return true;
        case "quotaconfig":
          builder.setQuotaConfig(JsonUtils.stringToObject(value, QuotaConfig.class));
          return true;
        case "tierconfigs":
          builder.setTierConfigList(JsonUtils.stringToObject(value,
              new TypeReference<List<TierConfig>>() { }));
          return true;
        case "tunerconfigs":
          builder.setTunerConfigList(JsonUtils.stringToObject(value,
              new TypeReference<List<TunerConfig>>() { }));
          return true;
        case "fieldconfigs":
          builder.setFieldConfigList(JsonUtils.stringToObject(value,
              new TypeReference<List<FieldConfig>>() { }));
          return true;
        case "instanceassignmentconfigmap":
          builder.setInstanceAssignmentConfigMap(JsonUtils.stringToObject(value,
              new TypeReference<Map<String, InstanceAssignmentConfig>>() { }));
          return true;
        case "tagoverrideconfig":
          builder.setTagOverrideConfig(
              JsonUtils.stringToObject(value, TagOverrideConfig.class));
          return true;
        case "replicagroupstrategyconfig":
          builder.setReplicaGroupStrategyConfig(
              JsonUtils.stringToObject(value, ReplicaGroupStrategyConfig.class));
          return true;
        case "completionconfig":
          builder.setCompletionConfig(JsonUtils.stringToObject(value, CompletionConfig.class));
          return true;
        case "startreeindexconfigs":
          builder.setStarTreeIndexConfigs(JsonUtils.stringToObject(value,
              new TypeReference<List<StarTreeIndexConfig>>() { }));
          return true;
        case "segmentpartitionconfig":
          builder.setSegmentPartitionConfig(
              JsonUtils.stringToObject(value, SegmentPartitionConfig.class));
          return true;
        case "multicolumntextindexconfig":
          builder.setMultiColumnTextIndexConfig(
              JsonUtils.stringToObject(value, MultiColumnTextIndexConfig.class));
          return true;
        case "jsonindexconfigs":
          builder.setJsonIndexConfigs(JsonUtils.stringToObject(value,
              new TypeReference<Map<String, JsonIndexConfig>>() { }));
          return true;
        case "instancepartitionsmap":
          builder.setInstancePartitionsMap(JsonUtils.stringToObject(value,
              new TypeReference<Map<InstancePartitionsType, String>>() { }));
          return true;
        case "segmentassignmentconfigmap":
          builder.setSegmentAssignmentConfigMap(JsonUtils.stringToObject(value,
              new TypeReference<Map<String, SegmentAssignmentConfig>>() { }));
          return true;
        case "tablesamplers":
          builder.setTableSamplers(JsonUtils.stringToObject(value,
              new TypeReference<List<TableSamplerConfig>>() { }));
          return true;
        case "tieroverwrites":
          builder.setTierOverwrites(JsonUtils.stringToObject(value, JsonNode.class));
          return true;
        default:
          return false;
      }
    } catch (Exception e) {
      throw new DdlCompilationException(
          "Failed to parse JSON value for property '" + rawKey + "': " + e.getMessage(), e);
    }
  }

  private static int parseInt(String key, String value) {
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      throw new DdlCompilationException(
          "Property '" + key + "' must be an integer; got '" + value + "'.");
    }
  }

  private static boolean parseBool(String key, String value) {
    String trimmed = value.trim();
    if ("true".equalsIgnoreCase(trimmed)) {
      return true;
    }
    if ("false".equalsIgnoreCase(trimmed)) {
      return false;
    }
    throw new DdlCompilationException(
        "Property '" + key + "' must be 'true' or 'false'; got '" + value + "'.");
  }

  private static List<String> splitCsv(String value) {
    if (value == null || value.isEmpty()) {
      return Collections.emptyList();
    }
    String[] parts = value.split(",");
    List<String> result = new ArrayList<>(parts.length);
    for (String part : parts) {
      String trimmed = part.trim();
      if (!trimmed.isEmpty()) {
        result.add(trimmed);
      }
    }
    return result;
  }
}
