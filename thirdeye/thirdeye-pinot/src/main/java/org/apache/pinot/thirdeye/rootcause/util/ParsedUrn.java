/*
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

package org.apache.pinot.thirdeye.rootcause.util;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


public final class ParsedUrn {
  private static Map<String, String> OPERATOR_TO_FILTER = new HashMap<>();
  static {
    OPERATOR_TO_FILTER.put("!=", "!");
    OPERATOR_TO_FILTER.put("==", "");
    OPERATOR_TO_FILTER.put("=", "");
    OPERATOR_TO_FILTER.put("<=", "<=");
    OPERATOR_TO_FILTER.put("<", "<");
    OPERATOR_TO_FILTER.put(">=", ">=");
    OPERATOR_TO_FILTER.put(">", ">");
  }

  final List<String> prefixes;
  final Set<FilterPredicate> predicates;

  public ParsedUrn(List<String> prefixes) {
    this.prefixes = prefixes;
    this.predicates = Collections.emptySet();
  }

  public ParsedUrn(List<String> prefixes, Set<FilterPredicate> predicates) {
    this.prefixes = Collections.unmodifiableList(prefixes);
    this.predicates = Collections.unmodifiableSet(predicates);
  }

  public List<String> getPrefixes() {
    return prefixes;
  }

  public Set<FilterPredicate> getPredicates() {
    return predicates;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ParsedUrn parsedUrn = (ParsedUrn) o;
    return Objects.equals(prefixes, parsedUrn.prefixes) && Objects.equals(predicates, parsedUrn.predicates);
  }

  @Override
  public int hashCode() {
    return Objects.hash(prefixes, predicates);
  }

  /**
   * Convenience method to assert absence of filter predicates at runtime.
   *
   * @throws IllegalArgumentException if at least one filter predicate is present
   */
  public void assertPrefixOnly() throws IllegalArgumentException {
    if (!this.getPredicates().isEmpty()) {
      throw new IllegalArgumentException(String.format("Expected prefix only but got predicates %s", this.getPredicates()));
    }
  }

  /**
   * Return FilterPredicates as filters multimap.
   *
   * @return filter multimap from predicates
   */
  // TODO use FilterPredicates throughout RCA framework
  public Multimap<String, String> toFilters() {
    Multimap<String, String> filters = TreeMultimap.create();
    for (FilterPredicate predicate : this.predicates) {
      if (!OPERATOR_TO_FILTER.containsKey(predicate.operator)) {
        throw new IllegalArgumentException(String.format("Operator '%s' could not be translated to filter prefix", predicate.operator));
      }
      String prefix = OPERATOR_TO_FILTER.get(predicate.operator);
      filters.put(predicate.key, prefix + predicate.value);
    }
    return filters;
  }
}
