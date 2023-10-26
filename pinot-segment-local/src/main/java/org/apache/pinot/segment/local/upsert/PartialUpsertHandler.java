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
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.segment.readers.LazyRow;
import org.apache.pinot.segment.local.upsert.merger.OverwriteMerger;
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
  private final PartialUpsertMerger _defaultPartialUpsertMerger;
  private final List<String> _comparisonColumns;
  private final List<String> _primaryKeyColumns;

  public PartialUpsertHandler(Schema schema, Map<String, UpsertConfig.Strategy> partialUpsertStrategies,
      UpsertConfig.Strategy defaultPartialUpsertStrategy, List<String> comparisonColumns) {
    _defaultPartialUpsertMerger = PartialUpsertMergerFactory.getMerger(defaultPartialUpsertStrategy);
    _comparisonColumns = comparisonColumns;
    _primaryKeyColumns = schema.getPrimaryKeyColumns();

    for (Map.Entry<String, UpsertConfig.Strategy> entry : partialUpsertStrategies.entrySet()) {
      _column2Mergers.put(entry.getKey(), PartialUpsertMergerFactory.getMerger(entry.getValue()));
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
   *
   * @param prevRecord wrapper for previous record, which lazily reads column values of previous row and caches for
   *                   re-reads.
   * @param newRecord the new consumed record.
   */
  public void merge(LazyRow prevRecord, GenericRow newRecord) {
    for (String column : prevRecord.getColumnNames()) {
      if (!_primaryKeyColumns.contains(column)) {
        PartialUpsertMerger merger = _column2Mergers.getOrDefault(column, _defaultPartialUpsertMerger);
        // Non-overwrite mergers
        // (1) If the value of the previous is null value, skip merging and use the new value
        // (2) Else If the value of new value is null, use the previous value (even for comparison columns).
        // (3) Else If the column is not a comparison column, we applied the merged value to it.
        if (!(merger instanceof OverwriteMerger)) {
          Object prevValue = prevRecord.getValue(column);
          if (prevValue != null) {
            if (newRecord.isNullValue(column)) {
              // Note that we intentionally want to overwrite any previous _comparisonColumn value in the case of
              // using
              // multiple comparison columns. We never apply a merge function to it, rather we just take any/all
              // non-null comparison column values from the previous record, and the sole non-null comparison column
              // value from the new record.
              newRecord.putValue(column, prevValue);
              newRecord.removeNullValueField(column);
            } else if (!_comparisonColumns.contains(column)) {
              newRecord.putValue(column, merger.merge(prevValue, newRecord.getValue(column)));
            }
          }
        } else {
          // Overwrite mergers.
          // (1) If the merge strategy is Overwrite merger and newValue is not null, skip and use the new value
          // (2) Otherwise, if previous is not null, init columnReader and use the previous value.
          if (newRecord.isNullValue(column)) {
            Object prevValue = prevRecord.getValue(column);
            if (prevValue != null) {
              newRecord.putValue(column, prevValue);
              newRecord.removeNullValueField(column);
            }
          }
        }
      }
    }
  }
}
