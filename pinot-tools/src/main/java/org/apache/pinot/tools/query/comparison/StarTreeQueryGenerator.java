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
package org.apache.pinot.tools.query.comparison;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;


/**
 * Given a list of dimension and metric columns, and possible values for dimension columns:
 * Generate queries of the form: SELECT SUM(m1), SUM(m2)... WHERE d1='v1' AND d2='v2'... GROUP BY d3,d4...
 */
public class StarTreeQueryGenerator {
  private static final String SELECT = "SELECT ";
  private static final String FROM = " FROM ";
  private static final String WHERE = " WHERE ";
  private static final String GROUP_BY = " GROUP BY ";
  private static final String BETWEEN = " BETWEEN ";
  private static final String IN = " IN ";
  private static final String NOT_IN = " NOT IN ";
  private static final String AND = " AND ";

  private static final int MAX_NUM_AGGREGATIONS = 5;
  private static final int MAX_NUM_PREDICATES = 10;
  private static final int MAX_NUM_GROUP_BYS = 3;
  private static final int MAX_NUM_IN_VALUES = 5;
  private static final int SHUFFLE_THRESHOLD = 5 * MAX_NUM_IN_VALUES;
  private static final Random RANDOM = new Random();
  // Add more comparators here to generate them in the 'WHERE' clause.
  private static final List<String> COMPARATORS = Arrays.asList("=", "<>", "<", ">", "<=", ">=");

  private final String _tableName;
  private final List<String> _singleValueDimensionColumns;
  private final List<String> _metricColumns;
  private final Map<String, List<Object>> _singleValueDimensionValuesMap;
  private final List<String> _aggregationFunctions;

  public StarTreeQueryGenerator(String tableName, List<String> singleValueDimensionColumns, List<String> metricColumns,
      Map<String, List<Object>> singleValueDimensionValuesMap, List<String> aggregationFunctions) {
    _tableName = tableName;
    _singleValueDimensionColumns = singleValueDimensionColumns;
    _metricColumns = metricColumns;
    _singleValueDimensionValuesMap = singleValueDimensionValuesMap;
    _aggregationFunctions = aggregationFunctions;
  }

  /**
   * Generate one aggregation function for the given metric column.
   *
   * @param metricColumn metric column.
   * @return aggregation function.
   */
  private StringBuilder generateAggregation(String metricColumn) {
    StringBuilder stringBuilder =
        new StringBuilder(_aggregationFunctions.get(RANDOM.nextInt(_aggregationFunctions.size())));
    return stringBuilder.append('(').append(metricColumn).append(')');
  }

  /**
   * Generate the aggregation section of the query, returns at least one aggregation.
   *
   * @return aggregation section.
   */
  private StringBuilder generateAggregations() {
    StringBuilder stringBuilder = new StringBuilder();

    int numAggregations = RANDOM.nextInt(MAX_NUM_AGGREGATIONS) + 1;
    int numMetrics = _metricColumns.size();
    for (int i = 0; i < numAggregations; i++) {
      if (i != 0) {
        stringBuilder.append(',').append(' ');
      }
      stringBuilder.append(generateAggregation(_metricColumns.get(RANDOM.nextInt(numMetrics))));
    }

    return stringBuilder;
  }

  /**
   * Generate a comparison predicate for the given dimension column.
   *
   * @param dimensionColumn dimension column.
   * @return comparison predicate.
   */
  private StringBuilder generateComparisonPredicate(String dimensionColumn) {
    StringBuilder stringBuilder = new StringBuilder(dimensionColumn);

    stringBuilder.append(' ').append(COMPARATORS.get(RANDOM.nextInt(COMPARATORS.size()))).append(' ');

    List<Object> valueArray = _singleValueDimensionValuesMap.get(dimensionColumn);
    Object value = valueArray.get(RANDOM.nextInt(valueArray.size()));
    if (value instanceof String) {
      stringBuilder.append('\'').append(((String) value).replaceAll("'", "''")).append('\'');
    } else {
      stringBuilder.append(value);
    }

    return stringBuilder;
  }

  /**
   * Generate a between predicate for the given dimension column.
   *
   * @param dimensionColumn dimension column.
   * @return between predicate.
   */
  private StringBuilder generateBetweenPredicate(String dimensionColumn) {
    StringBuilder stringBuilder = new StringBuilder(dimensionColumn).append(BETWEEN);

    List<Object> valueArray = _singleValueDimensionValuesMap.get(dimensionColumn);
    Object value1 = valueArray.get(RANDOM.nextInt(valueArray.size()));
    Object value2 = valueArray.get(RANDOM.nextInt(valueArray.size()));

    Preconditions.checkState((value1 instanceof String && value2 instanceof String)
        || (value1 instanceof Number && value2 instanceof Number));

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

    return stringBuilder;
  }

  /**
   * Generate a in predicate for the given dimension column.
   *
   * @param dimensionColumn dimension column.
   * @return in predicate.
   */
  private StringBuilder generateInPredicate(String dimensionColumn) {
    StringBuilder stringBuilder = new StringBuilder(dimensionColumn);
    if (RANDOM.nextBoolean()) {
      stringBuilder.append(IN).append('(');
    } else {
      stringBuilder.append(NOT_IN).append('(');
    }

    List<Object> valueArray = _singleValueDimensionValuesMap.get(dimensionColumn);
    int size = valueArray.size();
    int numValues = Math.min(RANDOM.nextInt(MAX_NUM_IN_VALUES) + 1, size);
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
        indices.add(RANDOM.nextInt(size));
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

    return stringBuilder.append(')');
  }

  /**
   * Randomly generate the WHERE clause of the query, may return empty string.
   *
   * @return all predicates.
   */
  private StringBuilder generatePredicates() {
    int numPredicates = RANDOM.nextInt(MAX_NUM_PREDICATES + 1);
    if (numPredicates == 0) {
      return null;
    }

    StringBuilder stringBuilder = new StringBuilder(WHERE);

    int numDimensions = _singleValueDimensionColumns.size();
    for (int i = 0; i < numPredicates; i++) {
      if (i != 0) {
        stringBuilder.append(AND);
      }
      String dimensionName = _singleValueDimensionColumns.get(RANDOM.nextInt(numDimensions));
      switch (RANDOM.nextInt(3)) {
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

    return stringBuilder;
  }

  /**
   * Randomly generate the GROUP BY section, may return empty string.
   *
   * @return group by section.
   */
  private StringBuilder generateGroupBys() {
    int numGroupBys = RANDOM.nextInt(MAX_NUM_GROUP_BYS + 1);
    if (numGroupBys == 0) {
      return null;
    }

    StringBuilder stringBuilder = new StringBuilder(GROUP_BY);

    int numDimensions = _singleValueDimensionColumns.size();
    for (int i = 0; i < numGroupBys; i++) {
      if (i != 0) {
        stringBuilder.append(',').append(' ');
      }
      stringBuilder.append(_singleValueDimensionColumns.get(RANDOM.nextInt(numDimensions)));
    }

    return stringBuilder;
  }

  /**
   * Combine the various sections to form the overall query.
   *
   * @param aggregations aggregation section.
   * @param predicates predicate section.
   * @param groupBys group by section.
   * @return overall query.
   */
  private String buildQuery(StringBuilder aggregations, StringBuilder predicates, StringBuilder groupBys) {
    StringBuilder stringBuilder = new StringBuilder(SELECT).append(aggregations);
    stringBuilder.append(FROM).append(_tableName);
    if (predicates != null) {
      stringBuilder.append(predicates);
    }
    if (groupBys != null) {
      stringBuilder.append(groupBys);
    }
    return stringBuilder.toString();
  }

  /**
   * Randomly generate a query.
   *
   * @return Return the generated query.
   */
  public String nextQuery() {
    return buildQuery(generateAggregations(), generatePredicates(), generateGroupBys());
  }
}
