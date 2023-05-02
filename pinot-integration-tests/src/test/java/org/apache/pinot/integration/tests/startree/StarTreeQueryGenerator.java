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
package org.apache.pinot.integration.tests.startree;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;


/**
 * Given a list of dimension and metric columns, and possible values for dimension columns:
 * Generate queries of the form: SELECT SUM(m1), SUM(m2)... WHERE d1='v1' AND d2='v2'... GROUP BY d3,d4...
 */
public class StarTreeQueryGenerator {
  private static final String SELECT = "SELECT ";
  private static final String FROM = " FROM ";
  private static final String WHERE = " WHERE ";
  private static final String GROUP_BY = " GROUP BY ";
  private static final String ORDER_BY = " ORDER BY ";
  private static final String BETWEEN = " BETWEEN ";
  private static final String IN = " IN ";
  private static final String NOT_IN = " NOT IN ";
  private static final String AND = " AND ";

  private static final int MAX_NUM_AGGREGATIONS = 5;
  private static final int MAX_NUM_PREDICATES = 10;
  private static final int MAX_NUM_GROUP_BYS = 3;
  private static final int MAX_NUM_IN_VALUES = 5;
  private static final int SHUFFLE_THRESHOLD = 5 * MAX_NUM_IN_VALUES;
  // Add more comparators here to generate them in the 'WHERE' clause.
  private static final List<String> COMPARATORS = Arrays.asList("=", "<>", "<", ">", "<=", ">=");

  private final String _tableName;
  private final List<String> _singleValueDimensionColumns;
  private final List<String> _metricColumns;
  private final Map<String, List<Object>> _singleValueDimensionValuesMap;
  private final List<String> _aggregationFunctions;
  private final Random _random;

  public StarTreeQueryGenerator(String tableName, List<String> singleValueDimensionColumns, List<String> metricColumns,
      Map<String, List<Object>> singleValueDimensionValuesMap, List<String> aggregationFunctions, Random random) {
    _tableName = tableName;
    _singleValueDimensionColumns = singleValueDimensionColumns;
    _metricColumns = metricColumns;
    _singleValueDimensionValuesMap = singleValueDimensionValuesMap;
    _aggregationFunctions = aggregationFunctions;
    _random = random;
  }

  /**
   * Generate one aggregation function for the given metric column.
   *
   * @param metricColumn metric column.
   * @return aggregation function.
   */
  private String generateAggregation(String metricColumn) {
    return String.format("%s(%s)", _aggregationFunctions.get(_random.nextInt(_aggregationFunctions.size())),
        metricColumn);
  }

  /**
   * Generate the aggregation section of the query, returns at least one aggregation.
   *
   * @return aggregation section.
   */
  private String generateAggregations() {
    int numAggregations = _random.nextInt(MAX_NUM_AGGREGATIONS) + 1;
    int numMetrics = _metricColumns.size();
    String[] aggregations = new String[numAggregations];
    for (int i = 0; i < numAggregations; i++) {
      aggregations[i] = generateAggregation(_metricColumns.get(_random.nextInt(numMetrics)));
    }
    return StringUtils.join(aggregations, ", ");
  }

  /**
   * Generate a comparison predicate for the given dimension column.
   *
   * @param dimensionColumn dimension column.
   * @return comparison predicate.
   */
  private String generateComparisonPredicate(String dimensionColumn) {
    StringBuilder stringBuilder = new StringBuilder(dimensionColumn);

    stringBuilder.append(' ').append(COMPARATORS.get(_random.nextInt(COMPARATORS.size()))).append(' ');

    List<Object> valueArray = _singleValueDimensionValuesMap.get(dimensionColumn);
    Object value = valueArray.get(_random.nextInt(valueArray.size()));
    if (value instanceof String) {
      stringBuilder.append('\'').append(((String) value).replaceAll("'", "''")).append('\'');
    } else {
      stringBuilder.append(value);
    }

    return stringBuilder.toString();
  }

  /**
   * Generate a between predicate for the given dimension column.
   *
   * @param dimensionColumn dimension column.
   * @return between predicate.
   */
  private String generateBetweenPredicate(String dimensionColumn) {
    StringBuilder stringBuilder = new StringBuilder(dimensionColumn).append(BETWEEN);

    List<Object> valueArray = _singleValueDimensionValuesMap.get(dimensionColumn);
    Object value1 = valueArray.get(_random.nextInt(valueArray.size()));
    Object value2 = valueArray.get(_random.nextInt(valueArray.size()));

    Preconditions.checkState((value1 instanceof String && value2 instanceof String) || (value1 instanceof Number
        && value2 instanceof Number));

    if (value1 instanceof String) {
      if (((String) value1).compareTo((String) value2) < 0) {
        stringBuilder.append('\'').append(((String) value1).replaceAll("'", "''")).append('\'');
        stringBuilder.append(AND);
        stringBuilder.append('\'').append(((String) value2).replaceAll("'", "''")).append('\'');
      } else {
        stringBuilder.append('\'').append(((String) value2).replaceAll("'", "''")).append('\'');
        stringBuilder.append(AND);
        stringBuilder.append('\'').append(((String) value1).replaceAll("'", "''")).append('\'');
      }
    } else {
      if (((Number) value1).doubleValue() < ((Number) value2).doubleValue()) {
        stringBuilder.append(value1).append(AND).append(value2);
      } else {
        stringBuilder.append(value2).append(AND).append(value1);
      }
    }

    return stringBuilder.toString();
  }

  /**
   * Generate a in predicate for the given dimension column.
   *
   * @param dimensionColumn dimension column.
   * @return in predicate.
   */
  private String generateInPredicate(String dimensionColumn) {
    StringBuilder stringBuilder = new StringBuilder(dimensionColumn);
    if (_random.nextBoolean()) {
      stringBuilder.append(IN).append('(');
    } else {
      stringBuilder.append(NOT_IN).append('(');
    }

    List<Object> valueArray = _singleValueDimensionValuesMap.get(dimensionColumn);
    int size = valueArray.size();
    int numValues = Math.min(_random.nextInt(MAX_NUM_IN_VALUES) + 1, size);
    if (size < SHUFFLE_THRESHOLD) {
      // For smaller size values, use shuffle strategy.
      Collections.shuffle(valueArray);
      for (int i = 0; i < numValues; i++) {
        if (i != 0) {
          stringBuilder.append(',').append(' ');
        }
        Object value = valueArray.get(i);
        if (value instanceof String) {
          stringBuilder.append('\'').append(((String) value).replaceAll("'", "''")).append('\'');
        } else {
          stringBuilder.append(value);
        }
      }
    } else {
      // For larger size values, use random indices strategy.
      Set<Integer> indices = new HashSet<>();
      while (indices.size() < numValues) {
        indices.add(_random.nextInt(size));
      }
      boolean isFirst = true;
      for (int index : indices) {
        if (isFirst) {
          isFirst = false;
        } else {
          stringBuilder.append(',').append(' ');
        }
        Object value = valueArray.get(index);
        if (value instanceof String) {
          stringBuilder.append('\'').append(((String) value).replaceAll("'", "''")).append('\'');
        } else {
          stringBuilder.append(value);
        }
      }
    }

    return stringBuilder.append(')').toString();
  }

  /**
   * Randomly generate the WHERE clause of the query, may return {@code null}.
   *
   * @return all predicates.
   */
  @Nullable
  private String generatePredicates() {
    int numPredicates = _random.nextInt(MAX_NUM_PREDICATES + 1);
    if (numPredicates == 0) {
      return null;
    }

    StringBuilder stringBuilder = new StringBuilder(WHERE);

    int numDimensions = _singleValueDimensionColumns.size();
    for (int i = 0; i < numPredicates; i++) {
      if (i != 0) {
        stringBuilder.append(AND);
      }
      String dimensionName = _singleValueDimensionColumns.get(_random.nextInt(numDimensions));
      switch (_random.nextInt(3)) {
        case 0:
          stringBuilder.append(generateComparisonPredicate(dimensionName));
          break;
        case 1:
          stringBuilder.append(generateBetweenPredicate(dimensionName));
          break;
        default:
          stringBuilder.append(generateInPredicate(dimensionName));
          break;
      }
    }

    return stringBuilder.toString();
  }

  /**
   * Randomly generate the group-by columns, may return {@code null}.
   */
  private String generateGroupByColumns() {
    int numColumns = _random.nextInt(MAX_NUM_GROUP_BYS + 1);
    if (numColumns == 0) {
      return null;
    }

    List<String> dimensions = new ArrayList<>(_singleValueDimensionColumns);
    Collections.shuffle(dimensions);
    return StringUtils.join(dimensions.subList(0, numColumns), ", ");
  }

  /**
   * Combine the various sections to form the overall query.
   *
   * @param aggregations aggregation section.
   * @param predicates predicate section.
   * @param groupByColumns group-by columns.
   * @return overall query.
   */
  private String buildQuery(String aggregations, String predicates, String groupByColumns) {
    StringBuilder stringBuilder = new StringBuilder(SELECT);
    if (groupByColumns != null) {
      stringBuilder.append(groupByColumns).append(", ");
    }
    stringBuilder.append(aggregations);
    stringBuilder.append(FROM).append(_tableName);
    if (predicates != null) {
      stringBuilder.append(predicates);
    }
    if (groupByColumns != null) {
      stringBuilder.append(GROUP_BY).append(groupByColumns).append(ORDER_BY).append(groupByColumns);
    }
    return stringBuilder.toString();
  }

  /**
   * Randomly generate a query.
   *
   * @return Return the generated query.
   */
  public String nextQuery() {
    return buildQuery(generateAggregations(), generatePredicates(), generateGroupByColumns());
  }
}
