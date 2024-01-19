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

  private final AggregationFunctionType _functionType;
  private final String _column;

  public AggregationFunctionColumnPair(AggregationFunctionType functionType, String column) {
    _functionType = functionType;
    if (functionType == AggregationFunctionType.COUNT) {
      _column = STAR;
    } else {
      _column = column;
    }
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
    String[] parts = columnName.split(DELIMITER, 2);
    return fromFunctionAndColumnName(parts[0], parts[1]);
  }

  public static AggregationFunctionColumnPair fromAggregationConfig(StarTreeAggregationConfig aggregationConfig) {
    return fromFunctionAndColumnName(aggregationConfig.getAggregationFunction(), aggregationConfig.getColumnName());
  }

  /**
   * Return a new {@code AggregationFunctionColumnPair} from an existing functionColumnPair where the new pair
   * has the {@link AggregationFunctionType} set to the underlying stored type used in the segment or indexes.
   * @param functionColumnPair the existing functionColumnPair
   * @return the new functionColumnPair
   */
  public static AggregationFunctionColumnPair resolveToStoredType(
      AggregationFunctionColumnPair functionColumnPair) {
    AggregationFunctionType valueAggregationFunctionType = getStoredType(functionColumnPair.getFunctionType());
    return new AggregationFunctionColumnPair(valueAggregationFunctionType, functionColumnPair.getColumn());
  }

  /**
   * Returns the stored {@code AggregationFunctionType} used to create the underlying value in the segment or index.
   * Some aggregation functions share the same stored type but are used for different purposes in queries.
   * @param aggregationType the aggregation type used in a query
   * @return the underlying value aggregation type used in storage e.g. StarTree index
   */
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
      default:
        return aggregationType;
    }
  }

  private static AggregationFunctionColumnPair fromFunctionAndColumnName(String functionName, String columnName) {
    AggregationFunctionType functionType = AggregationFunctionType.getAggregationFunctionType(functionName);
    if (functionType == AggregationFunctionType.COUNT) {
      return COUNT_STAR;
    } else {
      return new AggregationFunctionColumnPair(functionType, columnName);
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
