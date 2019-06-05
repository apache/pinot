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
package org.apache.pinot.common.dataset;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.ZNRecord;


/**
 * Dataset metadata
 *
 * Dataset metadata is persisted under the ZK path:
 * {@code <cluster>/PROPERTYSTORE/CONFIG/DATASET/{datasetNameWithType}/metadata}
 *
 */
public class DatasetMetadata {
  private static final String DATASET_TABLE_MAPPING_KEY = "datasetToTablesMapping";

  String _datasetNameWithType;

  // A mapping of a dataset to a list of tables. Only this field is written to ZK
  Map<String, List<String>> _datasetMetadata;

  public DatasetMetadata(String datasetNameWithType) {
    _datasetNameWithType = datasetNameWithType;
    _datasetMetadata = new HashMap<>();
  }

  public DatasetMetadata(String datasetNameWithType, Map<String, List<String>> datasetMetadata) {
    _datasetNameWithType = datasetNameWithType;
    _datasetMetadata = datasetMetadata;
  }

  public String getDatasetNameWithType() {
    return _datasetNameWithType;
  }

  public List<String> getTableNamesFromDatasetName(String datasetNameWithType) {
    return _datasetMetadata.get(DATASET_TABLE_MAPPING_KEY);
  }

  public void addDatasetToTableMapping(String tableNameWithType) {
    List<String> tableNamesWithType = _datasetMetadata.get(DATASET_TABLE_MAPPING_KEY);
    if (tableNamesWithType == null) {
      tableNamesWithType = new ArrayList<>();
    }
    if (!tableNamesWithType.contains(tableNameWithType)) {
      tableNamesWithType.add(tableNameWithType);
    }
    Preconditions.checkState(tableNamesWithType.size() > 0);

    _datasetMetadata.put(DATASET_TABLE_MAPPING_KEY, tableNamesWithType);
  }

  public void removeDatasetToTableMapping(String tableName) {
    List<String> tableNames = _datasetMetadata.get(DATASET_TABLE_MAPPING_KEY);

    if (tableNames == null) {
      return;
    }

    if (tableNames.contains(tableName)) {
      tableNames.remove(tableName);
    }
  }

  public static DatasetMetadata fromZNRecord(ZNRecord znRecord) {
    return new DatasetMetadata(znRecord.getId(), znRecord.getListFields());
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_datasetNameWithType);
    znRecord.setListFields(_datasetMetadata);
    return znRecord;
  }
}
