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
package org.apache.pinot.segment.local.upsert.merger;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.segment.readers.LazyRow;
import org.apache.pinot.segment.local.upsert.merger.columnar.ForceOverwriteMerger;
import org.apache.pinot.segment.local.upsert.merger.columnar.OverwriteMerger;
import org.apache.pinot.segment.local.upsert.merger.columnar.PartialUpsertColumnMerger;
import org.apache.pinot.segment.local.upsert.merger.columnar.PartialUpsertColumnMergerFactory;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Default Partial upsert merger implementation.
 * PartialUpsertColumnarMerger iterates over each column and merges them based on the defined strategy per column in
 * table config.
 */
public class PartialUpsertColumnarMerger extends BasePartialUpsertMerger {
  private final PartialUpsertColumnMerger _defaultColumnValueMerger;
  private final Map<String, PartialUpsertColumnMerger> _column2Mergers = new HashMap<>();

  public PartialUpsertColumnarMerger(List<String> primaryKeyColumns, List<String> comparisonColumns,
      UpsertConfig upsertConfig) {
    super(primaryKeyColumns, comparisonColumns, upsertConfig);
    _defaultColumnValueMerger =
        PartialUpsertColumnMergerFactory.getMerger(upsertConfig.getDefaultPartialUpsertStrategy());
    Map<String, UpsertConfig.Strategy> partialUpsertStrategies = upsertConfig.getPartialUpsertStrategies();
    Preconditions.checkArgument(partialUpsertStrategies != null, "Partial upsert strategies must be configured");
    for (Map.Entry<String, UpsertConfig.Strategy> entry : partialUpsertStrategies.entrySet()) {
      _column2Mergers.put(entry.getKey(), PartialUpsertColumnMergerFactory.getMerger(entry.getValue()));
    }
  }

  /**
   * Merges records and returns the merged record.
   * We used a map to indicate all configured fields for partial upsert. For these fields
   * (1) If the prev value is null, return the new value
   * (2) If the prev record is not null, the new value is null, return the prev value.
   * (3) If neither values are not null, then merge the value and return.
   * For un-configured fields, they are using default override behavior, regardless null values.
   *
   * For example, overwrite merger will only override the prev value if the new value is not null.
   * Null values will override existing values if not configured. They can be ignored by using ignoreMerger.
   */
  @Override
  public void merge(LazyRow previousRow, GenericRow newRow, Map<String, Object> resultHolder) {
    for (String column : previousRow.getColumnNames()) {
      // Skip primary key and comparison columns
      if (_primaryKeyColumns.contains(column) || _comparisonColumns.contains(column)) {
        continue;
      }
      PartialUpsertColumnMerger merger = _column2Mergers.getOrDefault(column, _defaultColumnValueMerger);
      if (merger instanceof ForceOverwriteMerger) {
        // Force Overwrite mergers
        // If the merge strategy is Force Overwrite merger and prevValue is always overwritten by newValue
        Object prevValue = previousRow.getValue(column);
        resultHolder.put(column, merger.merge(prevValue, newRow.getValue(column)));
      } else if (!(merger instanceof OverwriteMerger)) {
        // Non-overwrite mergers
        // (1) If the value of the previous is null value, skip merging and use the new value
        // (2) Else If the value of new value is null, use the previous value (even for comparison columns)
        // (3) Else If the column is not a comparison column, we applied the merged value to it
        Object prevValue = previousRow.getValue(column);
        if (prevValue != null) {
          if (newRow.isNullValue(column)) {
            resultHolder.put(column, prevValue);
          } else {
            resultHolder.put(column, merger.merge(prevValue, newRow.getValue(column)));
          }
        }
      } else {
        // Overwrite mergers
        // (1) If the merge strategy is Overwrite merger and newValue is not null, skip and use the new value
        // (2) Otherwise, use previous value if it is not null
        if (newRow.isNullValue(column)) {
          Object prevValue = previousRow.getValue(column);
          if (prevValue != null) {
            resultHolder.put(column, prevValue);
          }
        }
      }
    }
  }
}
