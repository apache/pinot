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
package org.apache.pinot.query.planner.plannode;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.query.planner.logical.RexExpression;


/**
 * One entry of a {@link MatchNode}'s MATCH_RECOGNIZE pattern variable symbol table: a variable name and its
 * {@code DEFINE} predicate.
 *
 * <p>The variable's <em>ordinal</em> is its index in {@link MatchNode#getPatternSymbols()}. Everything that refers
 * to a pattern variable - {@link RowPattern.Symbol}, {@link RexExpression.PatternFieldRef} and
 * {@link MatchNode#getAfterMatchSkipToSymbolOrdinal()} - does so by that ordinal, so the operator never has to
 * string-match on {@link #getName()}.
 *
 * <p>Instances are immutable.
 */
public class PatternSymbol {
  private final String _name;
  private final RexExpression _definition;

  public PatternSymbol(String name, @Nullable RexExpression definition) {
    _name = name;
    _definition = definition;
  }

  /**
   * Pattern variable name as written in the {@code PATTERN} / {@code DEFINE} clauses. Informational only.
   */
  public String getName() {
    return _name;
  }

  /**
   * The {@code DEFINE} predicate for this variable, or {@code null} when the variable has no {@code DEFINE} entry.
   * Per SQL:2016 an undefined pattern variable matches every row.
   */
  @Nullable
  public RexExpression getDefinition() {
    return _definition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PatternSymbol)) {
      return false;
    }
    PatternSymbol that = (PatternSymbol) o;
    return Objects.equals(_name, that._name) && Objects.equals(_definition, that._definition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_name, _definition);
  }

  @Override
  public String toString() {
    return _definition != null ? _name + " AS " + _definition : _name;
  }
}
