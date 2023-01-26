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

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class FieldIndexConfigsUtil {
  private FieldIndexConfigsUtil() {
  }

  public static Map<String, FieldIndexConfigs> createIndexConfigsByColName(
      TableConfig tableConfig, Schema schema, boolean failIfUnrecognized) {
    Map<String, FieldIndexConfigs> oldConf = translateOldConfig(tableConfig, schema);

    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList == null) {
      return oldConf;
    }

    Map<String, FieldIndexConfigs> newConf =
        FieldIndexConfigs.readFieldIndexConfigByColumn(fieldConfigList, failIfUnrecognized, schema.getColumnNames());

    return mergeConf(oldConf, newConf);
  }

  private static Map<String, FieldIndexConfigs> translateOldConfig(TableConfig tableConfig, Schema schema) {
    Map<String, FieldIndexConfigs> result = new HashMap<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      String colName = fieldSpec.getName();
      FieldIndexConfigs.Builder builder = new FieldIndexConfigs.Builder();
      for (IndexType<?, ?, ?> indexType : IndexService.getInstance().getAllIndexes()) {
        IndexDeclaration<?> conf = indexType.deserializeSpreadConf(tableConfig, schema, colName);
        builder.addUnsafeDeclaration(indexType, conf);
      }
      result.put(colName, builder.build());
    }
    return result;
  }

  public static Map<String, FieldIndexConfigs> mergeConf(
      Map<String, FieldIndexConfigs> oldConfMap, Map<String, FieldIndexConfigs> newConfMap) {

    if (oldConfMap.isEmpty()) {
      return newConfMap;
    }
    if (newConfMap.isEmpty()) {
      return oldConfMap;
    }

    Map<String, FieldIndexConfigs> merged = new HashMap<>();

    Set<String> columns = Sets.union(oldConfMap.keySet(), newConfMap.keySet());

    for (String column : columns) {
      FieldIndexConfigs oldConf = oldConfMap.get(column);
      FieldIndexConfigs newConf = newConfMap.get(column);

      if (oldConf == null) {
        if (newConf != null) {
          merged.put(column, newConf);
        }
      } else if (newConf == null) {
        merged.put(column, oldConf);
      } else {
        FieldIndexConfigs.Builder builder = new FieldIndexConfigs.Builder();
        for (IndexType<?, ?, ?> indexType : IndexService.getInstance().getAllIndexes()) {
          mergeConfig(builder, oldConf, newConf, indexType, column);
        }
        merged.put(column, builder.build());
      }
    }
    return merged;
  }

  private static <C> void mergeConfig(FieldIndexConfigs.Builder builder, FieldIndexConfigs oldConf,
      FieldIndexConfigs newConf, IndexType<C, ?, ?> indexType, String column) {
    IndexDeclaration<C> oldIndexConf = oldConf.getConfig(indexType);
    IndexDeclaration<C> newIndexConf = newConf.getConfig(indexType);
    if (oldIndexConf.isDeclared()) {
      if (newIndexConf.isDeclared()) {
        throw new IllegalArgumentException("Column " + column + " contains both legacy and current configs for "
            + "index " + indexType);
      }
      builder.addDeclaration(indexType, oldIndexConf);
    } else if (newIndexConf.isDeclared()) {
      builder.addDeclaration(indexType, newIndexConf);
    }
  }

  public static Set<String> columnsWithIndexEnabled(IndexType<?, ?, ?> indexType,
      Map<String, FieldIndexConfigs> configByCol) {
    return configByCol.entrySet().stream()
        .filter(e -> e.getValue().getConfig(indexType).isEnabled())
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  public static Set<String> columnsWithIndexDisabled(Set<String> allColumns, IndexType<?, ?, ?> indexType,
      Map<String, FieldIndexConfigs> configByCol) {
    return Sets.difference(allColumns, columnsWithIndexEnabled(indexType, configByCol));
  }

  public static <C> Map<String, C> enableConfigByColumn(IndexType<C, ?, ?> indexType,
      Map<String, FieldIndexConfigs> configByCol) {
    return configByCol.entrySet().stream()
        .filter(e -> e.getValue().getConfig(indexType).isEnabled())
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getConfig(indexType).getEnabledConfig()));
  }
}
