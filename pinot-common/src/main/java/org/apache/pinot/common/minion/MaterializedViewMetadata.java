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
package org.apache.pinot.common.minion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Stores the relationship between a materialized-view (MV) table and its base tables, including
 * partition mappings that support N:M partition propagation.
 *
 * <p>Persisted in ZooKeeper under
 * {@code /CONFIGS/MATERIALIZED_VIEW/<mvTableNameWithType>} as a {@link ZNRecord}.
 *
 * <p>Fields stored in {@link ZNRecord#getSimpleFields()}:
 * <ul>
 *   <li>{@code baseTables} – JSON array of table name strings</li>
 *   <li>{@code timeRangeRefTable} – the table whose time column drives refresh</li>
 *   <li>{@code definedSql} – the user-defined SQL query that produces the MV</li>
 *   <li>{@code baseToMvPartitionMap} – JSON map: basePartitionId -&gt; set of mvPartitionIds</li>
 *   <li>{@code mvToBasePartitionMap} – JSON map: mvPartitionId -&gt; set of basePartitionIds</li>
 * </ul>
 *
 * <p>Thread-safety: instances are effectively immutable after construction.
 */
public class MaterializedViewMetadata {

  private static final String BASE_TABLES_KEY = "baseTables";
  private static final String TIME_RANGE_REF_TABLE_KEY = "timeRangeRefTable";
  private static final String DEFINED_SQL_KEY = "definedSql";
  private static final String BASE_TO_MV_PARTITION_MAP_KEY = "baseToMvPartitionMap";
  private static final String MV_TO_BASE_PARTITION_MAP_KEY = "mvToBasePartitionMap";

  private static final TypeReference<List<String>> STRING_LIST_TYPE =
      new TypeReference<List<String>>() { };
  private static final TypeReference<Map<Integer, Set<Integer>>> PARTITION_MAP_TYPE =
      new TypeReference<Map<Integer, Set<Integer>>>() { };

  private final String _mvTableNameWithType;

  // All tables that the MV query depends on.
  // TODO: support multi-table joins -- currently only a single source table is expected.
  private final List<String> _baseTables;

  // The table whose time column is used to determine the time range that needs refreshing.
  private final String _timeRangeRefTable;

  // The original user-defined SQL query that produces the MV aggregation.
  private final String _definedSql;

  private final Map<Integer, Set<Integer>> _baseToMvPartitionMap;
  private final Map<Integer, Set<Integer>> _mvToBasePartitionMap;

  public MaterializedViewMetadata(String mvTableNameWithType, List<String> baseTables,
      String timeRangeRefTable, String definedSql,
      Map<Integer, Set<Integer>> baseToMvPartitionMap,
      Map<Integer, Set<Integer>> mvToBasePartitionMap) {
    _mvTableNameWithType = mvTableNameWithType;
    _baseTables = baseTables;
    _timeRangeRefTable = timeRangeRefTable;
    _definedSql = definedSql;
    _baseToMvPartitionMap = baseToMvPartitionMap;
    _mvToBasePartitionMap = mvToBasePartitionMap;
  }

  public String getMvTableNameWithType() {
    return _mvTableNameWithType;
  }

  public List<String> getBaseTables() {
    return _baseTables;
  }

  public String getTimeRangeRefTable() {
    return _timeRangeRefTable;
  }

  public String getDefinedSql() {
    return _definedSql;
  }

  public Map<Integer, Set<Integer>> getBaseToMvPartitionMap() {
    return _baseToMvPartitionMap;
  }

  public Map<Integer, Set<Integer>> getMvToBasePartitionMap() {
    return _mvToBasePartitionMap;
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_mvTableNameWithType);
    try {
      znRecord.setSimpleField(BASE_TABLES_KEY, JsonUtils.objectToString(_baseTables));
      znRecord.setSimpleField(TIME_RANGE_REF_TABLE_KEY, _timeRangeRefTable);
      if (_definedSql != null) {
        znRecord.setSimpleField(DEFINED_SQL_KEY, _definedSql);
      }

      if (!_baseToMvPartitionMap.isEmpty()) {
        znRecord.setSimpleField(BASE_TO_MV_PARTITION_MAP_KEY,
            JsonUtils.objectToString(_baseToMvPartitionMap));
      }
      if (!_mvToBasePartitionMap.isEmpty()) {
        znRecord.setSimpleField(MV_TO_BASE_PARTITION_MAP_KEY,
            JsonUtils.objectToString(_mvToBasePartitionMap));
      }
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize MaterializedViewMetadata", e);
    }
    return znRecord;
  }

  public static MaterializedViewMetadata fromZNRecord(ZNRecord znRecord) {
    String mvTableNameWithType = znRecord.getId();
    try {
      String baseTablesJson = znRecord.getSimpleField(BASE_TABLES_KEY);
      List<String> baseTables = baseTablesJson != null
          ? JsonUtils.stringToObject(baseTablesJson, STRING_LIST_TYPE)
          : Collections.emptyList();

      String timeRangeRefTable = znRecord.getSimpleField(TIME_RANGE_REF_TABLE_KEY);
      String definedSql = znRecord.getSimpleField(DEFINED_SQL_KEY);

      String baseToMvJson = znRecord.getSimpleField(BASE_TO_MV_PARTITION_MAP_KEY);
      Map<Integer, Set<Integer>> baseToMvPartitionMap = baseToMvJson != null
          ? JsonUtils.stringToObject(baseToMvJson, PARTITION_MAP_TYPE)
          : new HashMap<>();

      String mvToBaseJson = znRecord.getSimpleField(MV_TO_BASE_PARTITION_MAP_KEY);
      Map<Integer, Set<Integer>> mvToBasePartitionMap = mvToBaseJson != null
          ? JsonUtils.stringToObject(mvToBaseJson, PARTITION_MAP_TYPE)
          : new HashMap<>();

      return new MaterializedViewMetadata(mvTableNameWithType, baseTables, timeRangeRefTable,
          definedSql, baseToMvPartitionMap, mvToBasePartitionMap);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to deserialize MaterializedViewMetadata from ZNRecord", e);
    }
  }
}
