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

  private ComparisonColumns mergeComparisonColumnValues() {
    // first, we'll merge the values of this new row with the comparison values from any previous upsert
    ComparisonColumns inboundColumnValues = (ComparisonColumns) _comparisonValue;
    ComparisonColumns existingColumnValues = inboundColumnValues.getOther();
    if (existingColumnValues == null) {
      return inboundColumnValues;
    }

    Map<String, ComparisonColumn> mergedComparisonColumns = existingColumnValues.getComparisonColumns();

    for (Map.Entry<String, ComparisonColumn> columnEntry : inboundColumnValues.getComparisonColumns().entrySet()) {
      ComparisonColumn inboundValue = columnEntry.getValue();
      ComparisonColumn existingValue = mergedComparisonColumns.get(inboundValue.columnName);

      if (existingValue == null) {
        mergedComparisonColumns.put(inboundValue.columnName,
            new ComparisonColumn(inboundValue.columnName, inboundValue.comparisonValue, inboundValue.isNull));
        continue;
      }

      int comparisonResult = inboundValue.comparisonValue.compareTo(existingValue.comparisonValue);
      Comparable comparisonValue = comparisonResult >= 0 ? inboundValue.comparisonValue : existingValue.comparisonValue;

      mergedComparisonColumns.put(inboundValue.columnName,
          new ComparisonColumn(inboundValue.columnName, comparisonValue));

    }
    return new ComparisonColumns(mergedComparisonColumns);
  }
}
