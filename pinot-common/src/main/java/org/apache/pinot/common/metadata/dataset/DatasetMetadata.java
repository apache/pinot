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
package org.apache.pinot.common.metadata.dataset;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.helix.ZNRecord;


/**
 * Dataset metadata.
 *
 * Note: Dataset metadata contains the internal metadata on dataset while dataset config will contain user's
 * configuration on dataset.
 *
 *
 * Dataset metadata is persisted under the ZK path:
 * {@code <cluster>/PROPERTYSTORE/CONFIG/DATASET_METADATA/{datasetNameWithType}}
 *
 */
public class DatasetMetadata {
  private static final String TABLE_NAMES_KEY = "tableNames";

  String _datasetNameWithType;

  // A list of tables that are associated to this dataset.
  List<String> _tableNamesWithType;

  public DatasetMetadata(@Nonnull String datasetNameWithType) {
    _datasetNameWithType = datasetNameWithType;
    _tableNamesWithType = new ArrayList<>();
  }

  public DatasetMetadata(@Nonnull String datasetNameWithType, @Nonnull List<String> datasetToTablesMapping) {
    _datasetNameWithType = datasetNameWithType;
    _tableNamesWithType = datasetToTablesMapping;
  }

  public String getDatasetNameWithType() {
    return _datasetNameWithType;
  }

  public List<String> getTableNamesWithType() {
    return _tableNamesWithType;
  }

  public void addTableName(String tableNameWithType) {
    if (!_tableNamesWithType.contains(tableNameWithType)) {
      _tableNamesWithType.add(tableNameWithType);
    }
  }

  public void removeTableName(String tableNameWithType) {
    if (_tableNamesWithType.contains(tableNameWithType)) {
      _tableNamesWithType.remove(tableNameWithType);
    }
  }

  public static DatasetMetadata fromZNRecord(ZNRecord znRecord) {
    Map<String, List<String>> listFields = znRecord.getListFields();
    return new DatasetMetadata(znRecord.getId(), listFields.get(TABLE_NAMES_KEY));
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_datasetNameWithType);
    znRecord.setListField(TABLE_NAMES_KEY, _tableNamesWithType);
    return znRecord;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatasetMetadata that = (DatasetMetadata) o;
    return Objects.equals(_datasetNameWithType, that._datasetNameWithType) && Objects
        .equals(_tableNamesWithType, that._tableNamesWithType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_datasetNameWithType, _tableNamesWithType);
  }
}
