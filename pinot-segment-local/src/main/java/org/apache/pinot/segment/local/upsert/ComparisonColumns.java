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
import java.util.TreeMap;


@SuppressWarnings({"rawtypes", "unchecked"})
public class ComparisonColumns implements Comparable<ComparisonColumns> {
  private Map<String, Comparable> _comparisonColumns;

  public ComparisonColumns(Map<String, Comparable> comparisonColumns) {
    _comparisonColumns = comparisonColumns;
  }

  public Map<String, Comparable> getComparisonColumns() {
    return _comparisonColumns;
  }

  @Override
  public int compareTo(ComparisonColumns other) {
    if (_comparisonColumns.isEmpty()) {
      // All comparison columns were null. If all prior columns were null, we keep the record since this emulates the
      // same behavior in the case where only a single comparison column is used.
      if (other.getComparisonColumns().isEmpty()) {
        return 0;
      }
      return -1;
    }

    // _comparisonColumns will only have one value for inbound data, as only one non-null comparison value can be
    // sent with an inbound doc
    Map.Entry<String, Comparable> columnEntry = _comparisonColumns.entrySet().iterator().next();
    Comparable otherComparisonValue = other.getComparisonColumns().get(columnEntry.getKey());
    int comparisonResult;

    if (otherComparisonValue == null) {
      // Keep this record because the existing record has no value for the same comparison column, therefore the
      // (lack of) existing value could not possibly cause the new value to be rejected.
      comparisonResult = 1;
    } else {
      comparisonResult = columnEntry.getValue().compareTo(otherComparisonValue);
    }

    if (comparisonResult >= 0) {
      _comparisonColumns = merge(other.getComparisonColumns(), _comparisonColumns);
    }

    return comparisonResult;
  }

  public static Map<String, Comparable> merge(Map<String, Comparable> current, Map<String, Comparable> next) {
    // merge the values of this new row with the comparison values from any previous upsert. This should only be
    // called in the case where next.compareTo(current) >= 0
    if (current == null) {
      return next;
    }

    // Create a shallow copy so {@param current} is unmodified
    Map<String, Comparable> mergedComparisonColumns = new TreeMap<>(current);

    for (Map.Entry<String, Comparable> columnEntry : next.entrySet()) {
      Comparable inboundValue = columnEntry.getValue();
      String columnName = columnEntry.getKey();
      Comparable existingValue = mergedComparisonColumns.get(columnName);

      if (existingValue == null) {
        mergedComparisonColumns.put(columnName, inboundValue);
        continue;
      }

      int comparisonResult = inboundValue.compareTo(existingValue);
      Comparable comparisonValue =
          comparisonResult >= 0 ? inboundValue : existingValue;

      mergedComparisonColumns.put(columnName, comparisonValue);
    }
    return mergedComparisonColumns;
  }
}
