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
package org.apache.pinot.common.catalog;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.pinot.common.collections.FlatViewList;


/// A custom name matcher that, although matches names based on config-provided case-sensitiveness, always reports to be
/// case-sensitive so Calcite does not transform identifiers to lower case.
///
/// It is an improved implementation of Calcite's SqlNameMatchers.BaseMatcher that always true when asked if
/// isCaseSensitive() even when actual lookups are case-insensitive.
///
/// See [PinotCatalogReader] comments for more context.
public class PinotNameMatcher implements SqlNameMatcher {

  private final boolean _caseSensitive;

  public PinotNameMatcher(boolean caseSensitive) {
    _caseSensitive = caseSensitive;
  }

  @Override
  public boolean isCaseSensitive() {
    return true;
  }

  @Override
  public boolean matches(String string, String name) {
    return _caseSensitive ? string.equals(name) : string.equalsIgnoreCase(name);
  }

  @Nullable
  @Override
  public <K extends List<String>, V> V get(Map<K, V> map, List<String> prefixNames, List<String> names) {
    if (_caseSensitive) {
      List<String> key = concat(prefixNames, names);
      return map.get(key);
    } else {
      for (Map.Entry<K, V> entry : map.entrySet()) {
        if (listMatches(prefixNames, names, entry.getKey())) {
          return entry.getValue();
        }
      }

      return null;
    }
  }

  @Override
  public String bestString() {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public RelDataTypeField field(RelDataType rowType, String fieldName) {
    return rowType.getField(fieldName, _caseSensitive, false);
  }

  @Override
  public int frequency(Iterable<String> names, String name) {
    int n = 0;

    for (String s : names) {
      if (this.matches(s, name)) {
        ++n;
      }
    }

    return n;
  }

  @Override
  public Set<String> createSet() {
    return (this.isCaseSensitive() ? new LinkedHashSet<>() : new TreeSet<>(String.CASE_INSENSITIVE_ORDER));
  }

  private static List<String> concat(List<String> prefixNames, List<String> names) {
    /// Calcite's original code at `SqlNameMatchers.BaseMatcher.concat` creates a new list with the concatenation of
    /// prefixNames and names, leading to an extra allocation and potential copy of the values that we can avoid when
    /// the only purpose of the returning list is to iterate over its elements during an equality check. FlatViewList
    /// is a custom implementation that allows to iterate over the element of the two input lists without allocation a
    /// new one.
    return prefixNames.isEmpty() ? names : new FlatViewList<>(prefixNames, names);
  }

  /// Checks if the concatenation of two string lists matches a third list.
  /// This is adapted from the original `SqlNameMatchers.BaseMatcher.listMatches(list0, list1)` that allocates a new
  /// list with the concatenation of prefixes and names before checking if it matches the third list. Checking values
  /// directly from the two input lists avoids the need to allocate a new list and copy the values.
  protected boolean listMatches(List<String> firstList0, List<String> firstList1, List<String> secondList) {
    int firstListSize = firstList0.size() + firstList1.size();
    if (firstListSize != secondList.size()) {
      return false;
    } else {
      int breakIndex = firstList0.size();
      for (int i = 0; i < firstListSize; i++) {
        String s0 = i < breakIndex ? firstList0.get(i) : firstList1.get(i - breakIndex);
        String s1 = secondList.get(i);
        if (!matches(s0, s1)) {
          return false;
        }
      }

      return true;
    }
  }
}
