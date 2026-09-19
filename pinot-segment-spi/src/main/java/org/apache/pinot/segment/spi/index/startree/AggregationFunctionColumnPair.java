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
package org.apache.pinot.segment.spi.index.startree;

import java.util.Comparator;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.StarTreeAggregationConfig;


public class AggregationFunctionColumnPair implements Comparable<AggregationFunctionColumnPair> {
  public static final String DELIMITER = "__";
  public static final String STAR = "*";
  public static final AggregationFunctionColumnPair COUNT_STAR =
      new AggregationFunctionColumnPair(AggregationFunctionType.COUNT, STAR);
  public static final String COUNT_STAR_NAME = COUNT_STAR.toColumnName();

  private final AggregationFunctionType _functionType;
  private final String _column;

  public AggregationFunctionColumnPair(AggregationFunctionType functionType, String column) {
    this(functionType, column, false);
  }

  private AggregationFunctionColumnPair(AggregationFunctionType functionType, String column,
      boolean preserveCountColumn) {
    _functionType = functionType;
    // A regular star-tree counts rows rather than values, so every COUNT collapses to COUNT(*). A null-aware star-tree
    // instead stores the count of the non-null values of a specific column, which requires keeping the column name.
    if (functionType == AggregationFunctionType.COUNT && !preserveCountColumn) {
      _column = STAR;
    } else {
      _column = column;
    }
  }

  /// Returns the pair representing `COUNT(column)`, counting only the non-null values of the column.
  ///
  /// Unlike the regular constructor, which normalizes every `COUNT` to [#COUNT_STAR], this keeps the column so that a
  /// null-aware star-tree can pre-aggregate per-column non-null counts.
  public static AggregationFunctionColumnPair countColumn(String column) {
    return new AggregationFunctionColumnPair(AggregationFunctionType.COUNT, column, true);
  }

  public AggregationFunctionType getFunctionType() {
    return _functionType;
  }

  public String getColumn() {
    return _column;
  }

  public String toColumnName() {
    return toColumnName(_functionType, _column);
  }

  public static String toColumnName(AggregationFunctionType functionType, String column) {
    return functionType.getName() + DELIMITER + column;
  }

  public static AggregationFunctionColumnPair fromColumnName(String columnName) {
    return fromColumnName(columnName, false);
  }

  /// Parses a function-column pair name such as `sum__col`.
  ///
  /// When `preserveCountColumn` is `false`, `count__col` resolves to [#COUNT_STAR], matching how a regular star-tree
  /// stores counts. Pass `true` for a null-aware star-tree, where `count__col` denotes the non-null count of `col`.
  public static AggregationFunctionColumnPair fromColumnName(String columnName, boolean preserveCountColumn) {
    String[] parts = columnName.split(DELIMITER, 2);
    return fromFunctionAndColumnName(parts[0], parts[1], preserveCountColumn);
  }

  /// Builds a pair from an aggregation config. See [#fromColumnName] for the meaning of `preserveCountColumn`.
  public static AggregationFunctionColumnPair fromAggregationConfig(StarTreeAggregationConfig aggregationConfig,
      boolean preserveCountColumn) {
    return fromFunctionAndColumnName(aggregationConfig.getAggregationFunction(), aggregationConfig.getColumnName(),
        preserveCountColumn);
  }

  /// Return a new `AggregationFunctionColumnPair` from an existing functionColumnPair where the new pair
  /// has the [AggregationFunctionType] set to the underlying stored type used in the segment or indexes.
  /// @param functionColumnPair the existing functionColumnPair
  /// @return the new functionColumnPair
  public static AggregationFunctionColumnPair resolveToStoredType(AggregationFunctionColumnPair functionColumnPair) {
    AggregationFunctionType functionType = functionColumnPair.getFunctionType();
    AggregationFunctionType storedType = getStoredType(functionType);
    // Already in stored form. Returning it as-is also preserves the column of a per-column COUNT, which the
    // constructor would otherwise normalize back to STAR.
    if (storedType == functionType) {
      return functionColumnPair;
    }
    return new AggregationFunctionColumnPair(storedType, functionColumnPair.getColumn());
  }

  /// Returns the stored `AggregationFunctionType` used to create the underlying value in the segment or index.
  /// Some aggregation functions share the same stored type but are used for different purposes in queries.
  /// @param aggregationType the aggregation type used in a query
  /// @return the underlying value aggregation type used in storage e.g. StarTree index
  public static AggregationFunctionType getStoredType(AggregationFunctionType aggregationType) {
    switch (aggregationType) {
      case DISTINCTCOUNTRAWHLL:
        return AggregationFunctionType.DISTINCTCOUNTHLL;
      case PERCENTILERAWEST:
        return AggregationFunctionType.PERCENTILEEST;
      case PERCENTILERAWTDIGEST:
        return AggregationFunctionType.PERCENTILETDIGEST;
      case DISTINCTCOUNTRAWTHETASKETCH:
        return AggregationFunctionType.DISTINCTCOUNTTHETASKETCH;
      case DISTINCTCOUNTRAWHLLPLUS:
        return AggregationFunctionType.DISTINCTCOUNTHLLPLUS;
      case DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH:
      case AVGVALUEINTEGERSUMTUPLESKETCH:
      case SUMVALUESINTEGERSUMTUPLESKETCH:
        return AggregationFunctionType.DISTINCTCOUNTTUPLESKETCH;
      case DISTINCTCOUNTRAWCPCSKETCH:
        return AggregationFunctionType.DISTINCTCOUNTCPCSKETCH;
      case DISTINCTCOUNTRAWULL:
        return AggregationFunctionType.DISTINCTCOUNTULL;
      // TODO: Add type specific value aggregators for MIN / MAX / SUM and use those automatically for star-tree indexes
      //       based on the column type. For now, fall back to the default double-based star-tree index if one exists.
      case MINLONG:
        return AggregationFunctionType.MIN;
      case MAXLONG:
        return AggregationFunctionType.MAX;
      case SUMLONG:
      case SUMINT:
        return AggregationFunctionType.SUM;
      default:
        return aggregationType;
    }
  }

  private static AggregationFunctionColumnPair fromFunctionAndColumnName(String functionName, String columnName,
      boolean preserveCountColumn) {
    AggregationFunctionType functionType = AggregationFunctionType.getAggregationFunctionType(functionName);
    if (functionType == AggregationFunctionType.COUNT && !preserveCountColumn) {
      return COUNT_STAR;
    } else {
      return new AggregationFunctionColumnPair(functionType, columnName, preserveCountColumn);
    }
  }

  @Override
  public int hashCode() {
    return 31 * _functionType.hashCode() + _column.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof AggregationFunctionColumnPair) {
      AggregationFunctionColumnPair anotherPair = (AggregationFunctionColumnPair) obj;
      return _functionType == anotherPair._functionType && _column.equals(anotherPair._column);
    }
    return false;
  }

  @Override
  public String toString() {
    return toColumnName();
  }

  @Override
  public int compareTo(AggregationFunctionColumnPair other) {
    return Comparator.comparing((AggregationFunctionColumnPair o) -> o._column)
        .thenComparing((AggregationFunctionColumnPair o) -> o._functionType).compare(this, other);
  }
}
