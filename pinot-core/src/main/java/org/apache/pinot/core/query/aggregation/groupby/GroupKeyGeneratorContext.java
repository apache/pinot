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
package org.apache.pinot.core.query.aggregation.groupby;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Structurally immutable metadata exposed to a [GroupKeyGeneratorProvider]. Collections are defensively copied, but
/// expression objects are borrowed query-lifetime values. The context intentionally excludes operators and segment
/// data sources so providers cannot retain broader query-lifetime objects.
@InterfaceAudience.LimitedPrivate("StarTree")
@InterfaceStability.Unstable
public final class GroupKeyGeneratorContext {
  private final List<GroupKeySpec> _groupKeySpecs;
  private final Map<ExpressionContext, Integer> _predicateCardinalityHints;
  private final int _numGroupsLimit;
  private final int _maxInitialResultHolderCapacity;
  private final boolean _nullHandlingEnabled;

  GroupKeyGeneratorContext(List<GroupKeySpec> groupKeySpecs,
      Map<ExpressionContext, Integer> predicateCardinalityHints, int numGroupsLimit,
      int maxInitialResultHolderCapacity, boolean nullHandlingEnabled) {
    _groupKeySpecs = List.copyOf(Objects.requireNonNull(groupKeySpecs));
    _predicateCardinalityHints = Map.copyOf(Objects.requireNonNull(predicateCardinalityHints));
    _numGroupsLimit = numGroupsLimit;
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;
    _nullHandlingEnabled = nullHandlingEnabled;
  }

  public List<GroupKeySpec> getGroupKeySpecs() {
    return _groupKeySpecs;
  }

  public Map<ExpressionContext, Integer> getPredicateCardinalityHints() {
    return _predicateCardinalityHints;
  }

  public int getNumGroupsLimit() {
    return _numGroupsLimit;
  }

  public int getMaxInitialResultHolderCapacity() {
    return _maxInitialResultHolderCapacity;
  }

  public boolean isNullHandlingEnabled() {
    return _nullHandlingEnabled;
  }

  /// Metadata for one group-by expression. Integral domains are exact physical-column bounds. Cardinality is a
  /// sizing hint because raw-column segment metadata can be approximate. `dictionaryEncoded` describes the forward
  /// index, so it is false for a raw forward index with a side dictionary.
  public record GroupKeySpec(ExpressionContext expression, DataType storedType, boolean singleValue,
                             boolean dictionaryEncoded, Optional<IntegralDomain> exactIntegralDomain,
                             OptionalInt cardinalityHint) {
    public GroupKeySpec {
      Objects.requireNonNull(expression);
      Objects.requireNonNull(storedType);
      Objects.requireNonNull(exactIntegralDomain);
      Objects.requireNonNull(cardinalityHint);
    }
  }

  /// Inclusive exact range for an integral physical column.
  public record IntegralDomain(long minInclusive, long maxInclusive) {
    public IntegralDomain {
      if (minInclusive > maxInclusive) {
        throw new IllegalArgumentException(
            "minInclusive (" + minInclusive + ") must not exceed maxInclusive (" + maxInclusive + ")");
      }
    }
  }
}
