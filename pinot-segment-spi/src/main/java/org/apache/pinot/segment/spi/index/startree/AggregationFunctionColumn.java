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
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.StarTreeAggregationConfig;


public class AggregationFunctionColumn implements Comparable<AggregationFunctionColumn> {
  public static final String DELIMITER = "__";
  public static final String STAR = "*";
  public static final AggregationFunctionColumn COUNT_STAR =
      new AggregationFunctionColumn(AggregationFunctionType.COUNT, STAR);

  private final AggregationFunctionType _functionType;
  private final String _column;
  private final Map<String, Object> _functionParameters;

  public AggregationFunctionColumn(AggregationFunctionType functionType, String column) {
    this(functionType, column, Map.of());
  }

  public AggregationFunctionColumn(AggregationFunctionType functionType, String column,
      Map<String, Object> functionParameters) {
    _functionType = functionType;
    if (functionType == AggregationFunctionType.COUNT) {
      _column = STAR;
    } else {
      _column = column;
    }
    _functionParameters = functionParameters;
  }

  public AggregationFunctionType getFunctionType() {
    return _functionType;
  }

  public String getColumn() {
    return _column;
  }

  @Nullable
  public Map<String, Object> getFunctionParameters() {
    return _functionParameters;
  }

  public String toColumnName() {
    return toColumnName(_functionType, _column);
  }

  public static String toColumnName(AggregationFunctionType functionType, String column) {
    return functionType.getName() + DELIMITER + column;
  }

  public static AggregationFunctionColumn fromColumnName(String columnName) {
    String[] parts = columnName.split(DELIMITER, 2);
    return fromFunctionAndColumnName(parts[0], parts[1]);
  }

  public static AggregationFunctionColumn fromAggregationConfig(StarTreeAggregationConfig aggregationConfig) {
    AggregationFunctionType functionType = AggregationFunctionType.getAggregationFunctionType(
        aggregationConfig.getAggregationFunction());
    if (functionType == AggregationFunctionType.COUNT) {
      return COUNT_STAR;
    } else {
      return new AggregationFunctionColumn(functionType, aggregationConfig.getColumnName(),
          aggregationConfig.getFunctionParameters());
    }
  }

  /**
   * Return a new {@code AggregationFunctionColumn} from an existing functionColumn where the new pair
   * has the {@link AggregationFunctionType} set to the underlying stored type used in the segment or indexes.
   * @param functionColumn the existing AggregationFunctionColumn
   * @return the new AggregationFunctionColumn with the stored type
   */
  public static AggregationFunctionColumn resolveToStoredType(AggregationFunctionColumn functionColumn) {
    AggregationFunctionType storedType = getStoredType(functionColumn.getFunctionType());
    return new AggregationFunctionColumn(storedType, functionColumn.getColumn(),
        functionColumn.getFunctionParameters());
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

  private static AggregationFunctionColumn fromFunctionAndColumnName(String functionName, String columnName) {
    AggregationFunctionType functionType = AggregationFunctionType.getAggregationFunctionType(functionName);
    if (functionType == AggregationFunctionType.COUNT) {
      return COUNT_STAR;
    } else {
      return new AggregationFunctionColumn(functionType, columnName);
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
    if (obj instanceof AggregationFunctionColumn) {
      AggregationFunctionColumn other = (AggregationFunctionColumn) obj;
      // TODO: Revisit this since it means that for aggregation functions where a certain config parameter need not be
      // checked to determine whether a query can be served by a star-tree index, we won't rebuild a star-tree index
      // if the parameter value is changed in the index configuration.
      return _functionType == other._functionType && _column.equals(other._column)
          && AggregationFunctionType.compareFunctionParametersForStarTree(_functionType, _functionParameters,
          other._functionParameters) == 0;
    }
    return false;
  }

  @Override
  public String toString() {
    return toColumnName();
  }

  @Override
  public int compareTo(AggregationFunctionColumn other) {
    int compareValue = Comparator.comparing((AggregationFunctionColumn o) -> o._column)
        .thenComparing((AggregationFunctionColumn o) -> o._functionType)
        .compare(this, other);

    // Only equal if configurations match
    if (compareValue != 0) {
      return compareValue;
    } else {
      return AggregationFunctionType.compareFunctionParametersForStarTree(_functionType, _functionParameters,
          other._functionParameters);
    }
  }
}
