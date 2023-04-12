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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;


public class IndexConfigDeserializer {
  private IndexConfigDeserializer() {
  }

  public static <C extends IndexConfig> ColumnConfigDeserializer<C> ifIndexingConfig(
      ColumnConfigDeserializer<C> delegate) {
    return ((tableConfig, schema) -> tableConfig.getIndexingConfig() != null
        ? delegate.deserialize(tableConfig, schema) : Collections.emptyMap());
  }

  /**
   * Returns a {@link ColumnConfigDeserializer} that always returns the same config object
   */
  public static <C extends IndexConfig> ColumnConfigDeserializer<C> always(C config) {
    return (tableConfig, schema) -> {
      Map<String, C> result = new HashMap<>();
      for (String col : schema.getColumnNames()) {
        result.put(col, config);
      }
      return result;
    };
  }

  public static <C extends IndexConfig> ColumnConfigDeserializer<C> alwaysDefault(IndexType<C, ?, ?> indexType) {
    return always(indexType.getDefaultConfig());
  }

  /**
   * Returns a {@link ColumnConfigDeserializer} that always returns whatever is returned by the function given as
   * parameter.
   */
  public static <C extends IndexConfig> ColumnConfigDeserializer<C> alwaysCall(BiFunction<TableConfig, Schema, C> map) {
    return (tableConfig, schema) -> {
      C config = map.apply(tableConfig, schema);
      Map<String, C> result = new HashMap<>();
      for (String col : schema.getColumnNames()) {
        result.put(col, config);
      }
      return result;
    };
  }

  /**
   * Returns a {@link ColumnConfigDeserializer} that reads a specific fieldName of the <pre>indexes</pre> attribute on
   * each FieldConfig.
   */
  public static <C extends IndexConfig> ColumnConfigDeserializer<C> fromIndexes(String fieldName, Class<C> aClass) {
    return (tableConfig, schema) -> {
      Map<String, C> result = new HashMap<>();
      List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
      if (fieldConfigList != null) {
        for (FieldConfig fc : fieldConfigList) {
          if (!fc.getIndexes().isObject()) {
            continue;
          }
          JsonNode jsonNode = fc.getIndexes().get(fieldName);
          if (jsonNode == null) {
            continue;
          }
          C config;
          try {
            config = JsonUtils.jsonNodeToObject(jsonNode, aClass);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
          result.put(fc.getName(), config);
        }
      }
      return result;
    };
  }

  public static <C extends IndexConfig> ColumnConfigDeserializer<C> fromIndexTypes(
      FieldConfig.IndexType configIndexType, BiFunction<TableConfig, FieldConfig, C> mapFunction) {
    return (tableConfig, schema) -> {
      List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
      if (fieldConfigList == null) {
        return Collections.emptyMap();
      }
      Map<String, C> result = new HashMap<>();
      for (FieldConfig fieldConfig : fieldConfigList) {
        if (!fieldConfig.getIndexTypes().contains(configIndexType)) {
          continue;
        }
        result.put(fieldConfig.getName(), mapFunction.apply(tableConfig, fieldConfig));
      }
      return result;
    };
  }

  public static <T, C extends IndexConfig> ColumnConfigDeserializer<C> fromCollection(
      Function<TableConfig, Collection<T>> extract, BiConsumer<Map<String, C>, T> consume) {
    return (tableConfig, schema) -> {
      Collection<T> col = extract.apply(tableConfig);
      if (col == null) {
        return Collections.emptyMap();
      }
      Map<String, C> result = new HashMap<>();
      for (T temp : col) {
        consume.accept(result, temp);
      }
      return result;
    };
  }

  public static <K, T, C extends IndexConfig> ColumnConfigDeserializer<C> fromMap(
      Function<TableConfig, Map<K, T>> extract, TriConsumer<K, T, C> consume) {
    return (tableConfig, schema) -> {
      Map<K, T> map = extract.apply(tableConfig);
      if (map == null) {
        return Collections.emptyMap();
      }
      Map<String, C> result = new HashMap<>();
      for (Map.Entry<K, T> entry : map.entrySet()) {
        consume.accept(result, entry.getKey(), entry.getValue());
      }
      return result;
    };
  }

  @FunctionalInterface
  public interface TriConsumer<K, T, C> {
    void accept(Map<String, C> accum, K key, T temp);
  }

  public static <C extends IndexConfig> ColumnConfigDeserializer<C> fromMap(
      Function<TableConfig, Map<String, C>> extract) {
    return (tableConfig, schema) -> {
      Map<String, C> result = extract.apply(tableConfig);
      if (result == null) {
        return Collections.emptyMap();
      }
      return result;
    };
  }
}
