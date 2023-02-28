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
  private Comparable[] _values;

  public ComparisonColumns(Comparable[] values) {
    _values = values;
  }

  public Comparable[] getValues() {
    return _values;
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
      for (int i = 0; i < other.getValues().length; i++) {
        if (other.getValues()[i] != null) {
          comparisonResult = -1;
          break;
        }
      }
    } else {
      Comparable comparisonValue = _values[comparableIndex];
      Comparable otherComparisonValue = other.getValues()[comparableIndex];

      if (otherComparisonValue == null) {
        // Keep this record because the existing record has no value for the same comparison column, therefore the
        // (lack of) existing value could not possibly cause the new value to be rejected.
        comparisonResult = 1;
      } else {
        comparisonResult = comparisonValue.compareTo(otherComparisonValue);
      }
    }

    if (comparisonResult >= 0) {
      // TODO(egalpin):  This method currently may have side-effects on _values. Depending on the comparison result,
      //  entities from {@param other} may be merged into _values. This really should not be done implicitly as part
      //  of compareTo, but has been implemented this way to minimize the changes required within all subclasses of
      //  {@link BasePartitionUpsertMetadataManager}. Ideally, this merge should only be triggered explicitly by
      //  implementations of {@link BasePartitionUpsertMetadataManager}.
      for (int i = 0; i < _values.length; i++) {
        // N.B. null check _must_ be here to prevent overwriting _values[i] with null from other._values[i], such
        // as in the case where this is the first time that a non-null value has been received for a given
        // comparableIndex after previously receiving non-null value for a different comparableIndex
        if (i != comparableIndex && other._values[i] != null) {
          _values[i] = other._values[i];
        }
      }
    }

    return comparisonResult;
  }

  private int getComparableIndex() {
    for (int i = 0; i < _values.length; i++) {
      if (_values[i] == null) {
        continue;
      }
      return i;
    }
    return -1;
  }
}
