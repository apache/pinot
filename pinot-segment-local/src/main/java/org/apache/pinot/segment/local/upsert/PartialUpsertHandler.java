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
package org.apache.pinot.segment.local.upsert;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.local.upsert.merger.PartialUpsertMerger;
import org.apache.pinot.segment.local.upsert.merger.PartialUpsertMergerFactory;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Handler for partial-upsert.
 */
public class PartialUpsertHandler {
  // _column2Mergers maintains the mapping of merge strategies per columns.
  private final Map<String, PartialUpsertMerger> _column2Mergers = new HashMap<>();

  public PartialUpsertHandler(Schema schema, Map<String, UpsertConfig.Strategy> partialUpsertStrategies,
      UpsertConfig.Strategy defaultPartialUpsertStrategy, String comparisonColumn) {
    for (Map.Entry<String, UpsertConfig.Strategy> entry : partialUpsertStrategies.entrySet()) {
      _column2Mergers.put(entry.getKey(), PartialUpsertMergerFactory.getMerger(entry.getValue()));
    }
    // For all physical columns (including date time columns) except for primary key columns and comparison column.
    // If no comparison column is configured, use main time column as the comparison time.
    for (String columnName : schema.getPhysicalColumnNames()) {
      if (!schema.getPrimaryKeyColumns().contains(columnName) && !_column2Mergers.containsKey(columnName)
          && !comparisonColumn.equals(columnName)) {
        _column2Mergers.put(columnName, PartialUpsertMergerFactory.getMerger(defaultPartialUpsertStrategy));
      }
    }
  }

  /**
   * Merges 2 records and returns the merged record.
   * We used a map to indicate all configured fields for partial upsert. For these fields
   * (1) If the prev value is null, return the new value
   * (2) If the prev record is not null, the new value is null, return the prev value.
   * (3) If neither values are not null, then merge the value and return.
   * For un-configured fields, they are using default override behavior, regardless null values.
   *
   * For example, overwrite merger will only override the prev value if the new value is not null.
   * Null values will override existing values if not configured. They can be ignored by using ignoreMerger.
   *
   * @param previousRecord the last derived full record during ingestion.
   * @param newRecord the new consumed record.
   * @return a new row after merge
   */
  public GenericRow merge(GenericRow previousRecord, GenericRow newRecord) {
    for (Map.Entry<String, PartialUpsertMerger> entry : _column2Mergers.entrySet()) {
      String column = entry.getKey();
      if (!previousRecord.isNullValue(column)) {
        if (newRecord.isNullValue(column)) {
          newRecord.putValue(column, previousRecord.getValue(column));
          newRecord.removeNullValueField(column);
        } else {
          newRecord.putValue(column,
              entry.getValue().merge(previousRecord.getValue(column), newRecord.getValue(column)));
        }
      }
    }
    return newRecord;
  }
}
