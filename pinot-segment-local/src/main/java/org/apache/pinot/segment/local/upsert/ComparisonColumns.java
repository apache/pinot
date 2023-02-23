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


@SuppressWarnings({"rawtypes", "unchecked"})
public class ComparisonColumns implements Comparable<ComparisonColumns> {
  private Comparable[] _comparisonColumns;

  public ComparisonColumns(Comparable[] comparisonColumns) {
    _comparisonColumns = comparisonColumns;
  }

  public Comparable[] getComparisonColumns() {
    return _comparisonColumns;
  }

  @Override
  public int compareTo(ComparisonColumns other) {
    // _comparisonColumns should only at most one non-null comparison value. If not, it is the user's responsibility.
    // There is no attempt to guarantee behavior in the case where there are multiple non-null values
    int comparisonResult;
    int comparableIndex = getComparableIndex();

    if (comparableIndex < 0) {
      // All comparison values were null.  This record is only ok to keep if all prior values were also null
      comparisonResult = 1;
      for (int i = 0; i < other.getComparisonColumns().length; i++) {
        if (other.getComparisonColumns()[i] != null) {
          comparisonResult = -1;
          break;
        }
      }
    } else {
      Comparable comparisonValue = _comparisonColumns[comparableIndex];
      Comparable otherComparisonValue = other.getComparisonColumns()[comparableIndex];

      if (otherComparisonValue == null) {
        // Keep this record because the existing record has no value for the same comparison column, therefore the
        // (lack of) existing value could not possibly cause the new value to be rejected.
        comparisonResult = 1;
      } else {
        comparisonResult = comparisonValue.compareTo(otherComparisonValue);
      }
    }

    if (comparisonResult >= 0) {
      _comparisonColumns = merge(other.getComparisonColumns(), _comparisonColumns);
    }

    return comparisonResult;
  }

  private int getComparableIndex() {
    for (int i = 0; i < _comparisonColumns.length; i++) {
      if (_comparisonColumns[i] == null) {
        continue;
      }
      return i;
    }
    return -1;
  }

  public static Comparable[] merge(Comparable[] current, Comparable[] next) {
    // Create a shallow copy so {@param current} is unmodified
    Comparable[] mergedComparisonColumns = new Comparable[current.length];

    for (int i = 0; i < mergedComparisonColumns.length; i++) {
      Comparable inboundValue = next[i];
      Comparable existingValue = current[i];

      if (existingValue == null) {
        mergedComparisonColumns[i] = inboundValue;
        continue;
      }

      if (inboundValue == null) {
        mergedComparisonColumns[i] = existingValue;
        continue;
      }

      int comparisonResult = inboundValue.compareTo(existingValue);
      Comparable comparisonValue = comparisonResult >= 0 ? inboundValue : existingValue;
      mergedComparisonColumns[i] = comparisonValue;
    }
    return mergedComparisonColumns;
  }
}
