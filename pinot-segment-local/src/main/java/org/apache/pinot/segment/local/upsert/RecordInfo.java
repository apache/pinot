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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.PrimaryKey;


@SuppressWarnings({"rawtypes", "unchecked"})
public class RecordInfo {
  private final PrimaryKey _primaryKey;
  private final int _docId;
  private final Comparable _comparisonValue;

  public RecordInfo(PrimaryKey primaryKey, int docId, @Nullable Comparable comparisonValue) {
    _primaryKey = primaryKey;
    _docId = docId;
    _comparisonValue = comparisonValue;
  }

  public PrimaryKey getPrimaryKey() {
    return _primaryKey;
  }

  public int getDocId() {
    return _docId;
  }

  public Comparable getComparisonValue() {
    if (_comparisonValue instanceof ComparisonColumns) {
      return mergeComparisonColumnValues();
    }
    return _comparisonValue;
  }

  public void reset() {
    if (_comparisonValue instanceof ComparisonColumns) {
      ((ComparisonColumns) _comparisonValue).reset();
    }
  }

  private ComparisonColumns mergeComparisonColumnValues() {
    // first, we'll merge the values of this new row with the comparison values from any previous upsert
    // Note that we only reach this code by way of predicate which confirms _comparisonValue is of type
    // ComparisonColumns, meaning that it cannot be null
    ComparisonColumns inboundColumnValues = (ComparisonColumns) _comparisonValue;
    ComparisonColumns existingColumnValues = inboundColumnValues.getOther();
    if (existingColumnValues == null) {
      return inboundColumnValues;
    }

    Map<String, ComparisonValue> mergedComparisonColumns = existingColumnValues.getComparisonColumns();

    for (Map.Entry<String, ComparisonValue> columnEntry : inboundColumnValues.getComparisonColumns().entrySet()) {
      ComparisonValue inboundValue = columnEntry.getValue();
      String columnName = columnEntry.getKey();
      ComparisonValue existingValue = mergedComparisonColumns.get(columnName);

      if (existingValue == null) {
        mergedComparisonColumns.put(columnName,
            new ComparisonValue(inboundValue.getComparisonValue(), inboundValue.isNull()));
        continue;
      }

      int comparisonResult = inboundValue.getComparisonValue().compareTo(existingValue.getComparisonValue());
      Comparable comparisonValue =
          comparisonResult >= 0 ? inboundValue.getComparisonValue() : existingValue.getComparisonValue();

      mergedComparisonColumns.put(columnName, new ComparisonValue(comparisonValue));
    }
    return new ComparisonColumns(mergedComparisonColumns);
  }
}
