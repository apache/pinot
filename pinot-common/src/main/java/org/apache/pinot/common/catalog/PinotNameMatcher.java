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

import com.google.common.collect.ImmutableList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * A custom name matcher that, although matches names based on config-provided case-sensitiveness, always reports to be
 * case-sensitive so Calcite does not transform identifiers to lower case.
 */
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

  @Override
  public <K extends List<String>, V> @Nullable V get(Map<K, V> map, List<String> prefixNames, List<String> names) {
    List<String> key = concat(prefixNames, names);
    if (_caseSensitive) {
      return map.get(key);
    } else {
      for (Map.Entry<K, V> entry : map.entrySet()) {
        if (this.listMatches(key, entry.getKey())) {
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

  @Override
  public @Nullable RelDataTypeField field(RelDataType rowType, String fieldName) {
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
    return (List<String>) (prefixNames.isEmpty() ? names : ImmutableList.builder()
        .addAll(prefixNames).addAll(names).build());
  }

  protected boolean listMatches(List<String> list0, List<String> list1) {
    if (list0.size() != list1.size()) {
      return false;
    } else {
      for (int i = 0; i < list0.size(); i++) {
        String s0 = list0.get(i);
        String s1 = list1.get(i);
        if (!this.matches(s0, s1)) {
          return false;
        }
      }

      return true;
    }
  }
}
