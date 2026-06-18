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

package org.apache.pinot.segment.spi.index;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;


public class FieldIndexConfigsUtil {
  private FieldIndexConfigsUtil() {
  }

  public static Map<String, FieldIndexConfigs> createIndexConfigsByColName(TableConfig tableConfig, Schema schema) {
    return createIndexConfigsByColName(tableConfig, schema, DefaultDeserializerProvider.INSTANCE);
  }

  public static Map<String, FieldIndexConfigs> createIndexConfigsByColName(
      TableConfig tableConfig, Schema schema, DeserializerProvider deserializerProvider) {
    Map<String, FieldIndexConfigs.Builder> builderMap = new HashMap<>();
    for (String columnName : schema.getColumnNames()) {
      builderMap.put(columnName, new FieldIndexConfigs.Builder());
    }
    for (IndexType<?, ?, ?> indexType : IndexService.getInstance().getAllIndexes()) {
      readConfig(builderMap, indexType, tableConfig, schema, deserializerProvider);
    }

    return builderMap.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().build()));
  }

  /// Builds a {@link FieldIndexConfigs} for a single column directly from one {@link FieldConfig}, without a
  /// `TableConfig` or `Schema`. The dictionary entry is derived from `fieldConfig.getEncodingType()`
  /// (RAW => disabled, otherwise default-enabled); every other index type is read from the modern
  /// `fieldConfig.getIndexes()` JSON (keyed by index pretty name), falling back to each type's default config.
  /// A `null` fieldConfig yields built-in defaults (dictionary enabled).
  ///
  /// This reads only the modern `indexes` format and never legacy `IndexingConfig` lists, so it suits
  /// synthetic columns (e.g. OPEN_STRUCT materialized children) that exist in no schema.
  public static FieldIndexConfigs fromFieldConfig(@Nullable FieldConfig fieldConfig, FieldSpec fieldSpec) {
    FieldIndexConfigs.Builder builder = new FieldIndexConfigs.Builder();
    boolean rawEncoded = fieldConfig != null && fieldConfig.getEncodingType() == FieldConfig.EncodingType.RAW;
    builder.add(StandardIndexes.dictionary(),
        rawEncoded ? DictionaryIndexConfig.DISABLED : DictionaryIndexConfig.DEFAULT);
    JsonNode indexes = fieldConfig != null ? fieldConfig.getIndexes() : null;
    for (IndexType<?, ?, ?> indexType : IndexService.getInstance().getAllIndexes()) {
      if (indexType.getId().equals(StandardIndexes.DICTIONARY_ID)) {
        continue;
      }
      addConfigFromIndexes(builder, indexType, indexes);
    }
    return builder.build();
  }

  private static <C extends IndexConfig> void addConfigFromIndexes(FieldIndexConfigs.Builder builder,
      IndexType<C, ?, ?> indexType, @Nullable JsonNode indexes) {
    JsonNode node = indexes != null ? indexes.get(indexType.getPrettyName()) : null;
    C config;
    if (node != null) {
      try {
        config = JsonUtils.jsonNodeToObject(node, indexType.getIndexConfigClass());
      } catch (IOException e) {
        throw new IllegalArgumentException(
            "Failed to parse '" + indexType.getPrettyName() + "' index config from FieldConfig", e);
      }
    } else {
      config = indexType.getDefaultConfig();
    }
    builder.add(indexType, config);
  }

  @FunctionalInterface
  public interface DeserializerProvider {
    <C extends IndexConfig> ColumnConfigDeserializer<C> get(IndexType<C, ?, ?> indexType);
  }

  private static <C extends IndexConfig> void readConfig(
      Map<String, FieldIndexConfigs.Builder> builderMap, IndexType<C, ?, ?> indexType,
      TableConfig tableConfig, Schema schema, DeserializerProvider deserializerProvider) {
    ColumnConfigDeserializer<C> deserializer = deserializerProvider.get(indexType);
    Map<String, C> deserialize = deserializer.deserialize(tableConfig, schema);

    for (Map.Entry<String, C> entry : deserialize.entrySet()) {
      FieldIndexConfigs.Builder colBuilder =
          builderMap.computeIfAbsent(entry.getKey(), key -> new FieldIndexConfigs.Builder());
      colBuilder.addUnsafe(indexType, entry.getValue());
    }
  }

  public static Set<String> columnsWithIndexEnabled(IndexType<?, ?, ?> indexType,
      Map<String, FieldIndexConfigs> configByCol) {
    return configByCol.entrySet().stream()
        .filter(e -> {
          IndexConfig config = e.getValue().getConfig(indexType);
          return config != null && config.isEnabled();
        })
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  /**
   * Returns the columns on the map that whose given index type is disabled.
   *
   * It is recommended to use {@link #columnsWithIndexDisabled(Set, IndexType, Map)} when the map may not have an entry
   * for all columns in the schema.
   */
  public static Set<String> columnsWithIndexDisabled(IndexType<?, ?, ?> indexType,
      Map<String, FieldIndexConfigs> configByCol) {
    return Sets.difference(configByCol.keySet(), columnsWithIndexEnabled(indexType, configByCol));
  }

  public static Set<String> columnsWithIndexDisabled(Set<String> allColumns, IndexType<?, ?, ?> indexType,
      Map<String, FieldIndexConfigs> configByCol) {
    return Sets.difference(allColumns, columnsWithIndexEnabled(indexType, configByCol));
  }

  public static <C extends IndexConfig> Map<String, C> enableConfigByColumn(IndexType<C, ?, ?> indexType,
      Map<String, FieldIndexConfigs> configByCol) {
    return configByCol.entrySet().stream()
        .filter(e -> {
          C config = e.getValue().getConfig(indexType);
          return config != null && config.isEnabled();
        })
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getConfig(indexType)));
  }

  private static class DefaultDeserializerProvider implements DeserializerProvider {
    public static final DefaultDeserializerProvider INSTANCE = new DefaultDeserializerProvider();

    @Override
    public <C extends IndexConfig> ColumnConfigDeserializer<C> get(IndexType<C, ?, ?> indexType) {
      return indexType::getConfig;
    }
  }
}
