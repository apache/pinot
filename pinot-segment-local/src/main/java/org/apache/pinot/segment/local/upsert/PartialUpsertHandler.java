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
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.readers.LazyRow;
import org.apache.pinot.segment.local.upsert.merger.PartialUpsertMerger;
import org.apache.pinot.segment.local.upsert.merger.PartialUpsertMergerFactory;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Handler for partial-upsert.
 *
 * This class is responsible for merging the new record with the previous record.
 * It uses the configured merge strategies to merge the columns. If no merge strategy is configured for a column,
 * it uses the default merge strategy.
 *
 * It is also possible to define a custom logic for merging rows by implementing {@link PartialUpsertMerger}.
 * If a merger for row is defined then it takes precedence and ignores column mergers.
 */
public class PartialUpsertHandler {
  private final List<String> _primaryKeyColumns;
  private final List<String> _comparisonColumns;
  private final PartialUpsertMerger _partialUpsertMerger;

  private final Map<String, Object> _defaultNullValues = new HashMap<>();

  public PartialUpsertHandler(Schema schema, List<String> comparisonColumns, UpsertConfig upsertConfig) {
    _primaryKeyColumns = schema.getPrimaryKeyColumns();
    _comparisonColumns = comparisonColumns;
    _partialUpsertMerger =
        PartialUpsertMergerFactory.getPartialUpsertMerger(_primaryKeyColumns, comparisonColumns, upsertConfig);
    // cache default null values to handle null merger results
    for (Map.Entry<String, FieldSpec> entry : schema.getFieldSpecMap().entrySet()) {
      String column = entry.getKey();
      FieldSpec fieldSpec = entry.getValue();
      if (fieldSpec.isSingleValueField()) {
        _defaultNullValues.put(column, fieldSpec.getDefaultNullValue());
      } else {
        _defaultNullValues.put(column, new Object[]{fieldSpec.getDefaultNullValue()});
      }
    }
  }

  public void merge(LazyRow previousRow, GenericRow newRow, Map<String, Object> resultHolder) {
    _partialUpsertMerger.merge(previousRow, newRow, resultHolder);

    // iterate over only merger results and update newRecord with merged values
    for (Map.Entry<String, Object> entry : resultHolder.entrySet()) {
      // skip primary key and comparison columns
      String column = entry.getKey();
      if (_primaryKeyColumns.contains(column) || _comparisonColumns.contains(column)) {
        continue;
      }
      setMergedValue(newRow, column, entry.getValue());
    }

    // handle comparison columns
    for (String column : _comparisonColumns) {
      if (newRow.isNullValue(column) && !previousRow.isNullValue(column)) {
        newRow.putValue(column, previousRow.getValue(column));
        newRow.removeNullValueField(column);
      }
    }
  }

  private void setMergedValue(GenericRow row, String column, @Nullable Object mergedValue) {
    if (mergedValue != null) {
      // remove null value field if it was set
      row.removeNullValueField(column);
      row.putValue(column, mergedValue);
    } else {
      row.putDefaultNullValue(column, _defaultNullValues.get(column));
    }
  }
}
