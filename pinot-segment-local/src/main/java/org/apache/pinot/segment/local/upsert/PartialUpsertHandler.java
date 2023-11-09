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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.segment.readers.LazyRow;
import org.apache.pinot.segment.local.upsert.merger.PartialUpsertMerger;
import org.apache.pinot.segment.local.upsert.merger.PartialUpsertMergerFactory;
import org.apache.pinot.segment.local.upsert.merger.columnar.PartialUpsertColumnMerger;
import org.apache.pinot.segment.local.upsert.merger.columnar.PartialUpsertColumnMergerFactory;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Handler for partial-upsert.
 */
public class PartialUpsertHandler {
  // _column2Mergers maintains the mapping of merge strategies per columns.
  private final Map<String, PartialUpsertColumnMerger> _column2Mergers = new HashMap<>();
  private final PartialUpsertColumnMerger _defaultPartialUpsertMerger;
  private final List<String> _comparisonColumns;
  private final List<String> _primaryKeyColumns;
  private final PartialUpsertMerger _partialUpsertMerger;

  public PartialUpsertHandler(Schema schema, List<String> comparisonColumns, UpsertConfig upsertConfig) {
    _defaultPartialUpsertMerger =
        PartialUpsertColumnMergerFactory.getMerger(upsertConfig.getDefaultPartialUpsertStrategy());
    _comparisonColumns = comparisonColumns;
    _primaryKeyColumns = schema.getPrimaryKeyColumns();

    _partialUpsertMerger =
        PartialUpsertMergerFactory.getPartialUpsertMerger(_primaryKeyColumns, comparisonColumns, upsertConfig);
  }

  @VisibleForTesting
  public PartialUpsertHandler(Schema schema, List<String> comparisonColumns, UpsertConfig upsertConfig,
      PartialUpsertMerger partialUpsertMerger) {
    _defaultPartialUpsertMerger =
        PartialUpsertColumnMergerFactory.getMerger(upsertConfig.getDefaultPartialUpsertStrategy());
    _comparisonColumns = comparisonColumns;
    _primaryKeyColumns = schema.getPrimaryKeyColumns();

    _partialUpsertMerger = partialUpsertMerger;
  }

  public void merge(LazyRow prevRecord, GenericRow newRecord, Map<String, Object> reuseMergerResult) {
    reuseMergerResult.clear();

    // merger current row with previously indexed row
    _partialUpsertMerger.merge(prevRecord, newRecord, reuseMergerResult);

    for (String column : prevRecord.getColumnNames()) {
      // no merger to apply on primary key columns
      if (_primaryKeyColumns.contains(column)) {
        continue;
      }
      // no merger to apply on comparison key column, use previous row's value if current is null
      if (_comparisonColumns.contains(column)) {
        if (newRecord.isNullValue(column) && !prevRecord.isNullValue(column)) {
          newRecord.putValue(column, prevRecord.getValue(column));
          newRecord.removeNullValueField(column);
        }
        continue;
      }

      // use merged column value from result map
      if (reuseMergerResult.containsKey(column)) {
        Object mergedValue = reuseMergerResult.get(column);
        if (mergedValue != null) {
          // remove null value field if it was set
          newRecord.removeNullValueField(column);
          newRecord.putValue(column, mergedValue);
        } else {
          // if column exists but mapped to a null value then merger result was null
          newRecord.addNullValueField(column);
        }
      }
    }
  }
}
