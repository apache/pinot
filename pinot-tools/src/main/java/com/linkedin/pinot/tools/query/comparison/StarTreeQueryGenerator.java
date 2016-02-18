/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools.query.comparison;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang.StringUtils;


/**
 * Given a list of dimension and metric columns, and possible values for dimension columns:
 * Generate queries of the form: SELECT SUM(m1), SUM(m2)... WHERE d1='v1' AND d2='v2'... GROUP BY d3,d4...
 */
public class StarTreeQueryGenerator {

  private static final int MAX_NUM_PREDICATES = 3;
  private static final int MAX_NUM_GROUP_BYS = 3;

  private static final String SELECT = "SELECT";
  private static final String FROM = "FROM";
  private static final String WHERE = "WHERE";
  private static final String GROUP_BY = "GROUP BY";
  private static final String EMPTY_STRING = "";

  private Random _random;

  private final String _tableName;
  private final List<String> _dimensionColumns;
  private final List<String> _metricColumns;
  private final Map<String, List<String>> _columnValues;

  // Add more functions here to generate them in the queries.
  private final List<String> _aggregationFunctions = Arrays.asList(new String[]{"SUM"});

  // Add more comparators here to generate them in the 'WHERE' clause.
  private final List<String> _comparators = Arrays.asList(new String[]{"="});

  public StarTreeQueryGenerator(String tableName, List<String> dimensionColumns, List<String> metricColumns,
      Map<String, List<String>> columnValues) {
    _tableName = tableName;
    _dimensionColumns = dimensionColumns;
    _metricColumns = metricColumns;
    _columnValues = columnValues;
    _random = new Random();
  }

  /**
   * Generate one aggregation string for the given column.
   * @param column
   * @return
   */
  private String generateAggregation(String column) {
    int max = _aggregationFunctions.size();
    return _aggregationFunctions.get(_random.nextInt(max)) + "(" + column + ")";
  }

  /**
   * Generate the aggregation section of the query, returns at least one aggregation.
   * @return
   */
  private String generateAggregations() {
    int numAggregations = _random.nextInt(_metricColumns.size() + 1);
    numAggregations = Math.max(numAggregations, 1);
    List<String> aggregations = new ArrayList<>(numAggregations);

    Collections.shuffle(_metricColumns);
    for (int i = 0; i < numAggregations; i++) {
      aggregations.add(generateAggregation(_metricColumns.get(i)));
    }

    return StringUtils.join(aggregations, ", ");
  }

  /**
   * Generate individual predicates that form the overall WHERE clause.
   * @param column
   * @return
   */
  private String generatePredicate(String column) {
    List<String> predicate = new ArrayList<>();

    predicate.add(column);
    predicate.add(_comparators.get(_random.nextInt(_comparators.size())));
    List<String> valueArray = _columnValues.get(column);

    String value = "'" + valueArray.get(_random.nextInt(valueArray.size())) + "'";
    predicate.add(value);

    return StringUtils.join(predicate, " ");
  }

  /**
   * Randomly generate the WHERE clause of the query, may return empty string
   * @return
   */
  private String generatePredicates() {
    int numDimensions = _dimensionColumns.size();
    int numPredicates = _random.nextInt(numDimensions + 1);
    numPredicates = Math.min(numPredicates, MAX_NUM_PREDICATES);
    if (numPredicates == 0) {
      return EMPTY_STRING;
    }

    List<String> predicates = new ArrayList<>(numPredicates);
    Collections.shuffle(_dimensionColumns);
    for (int i = 0; i < numPredicates; i++) {
      predicates.add(generatePredicate(_dimensionColumns.get(i)));
    }

    return WHERE + " " + StringUtils.join(predicates, " AND ");
  }

  /**
   * Randomly generate the GROUP BY section, may return empty string.
   * @return
   */
  private String generateGroupBys() {
    List<String> groupBys = new ArrayList<>();
    int numDimensions = _dimensionColumns.size();

    int numGroupBys = _random.nextInt(numDimensions + 1);
    numGroupBys = Math.min(numGroupBys, MAX_NUM_GROUP_BYS);
    if (numGroupBys == 0) {
      return EMPTY_STRING;
    }

    Collections.shuffle(_dimensionColumns);
    for (int i = 0; i < numGroupBys; i++) {
      groupBys.add(_dimensionColumns.get(i));
    }

    return GROUP_BY + " " + StringUtils.join(groupBys, ", ");
  }

  /**
   * Combine the various sections to form the overall query
   *
   * @param aggregations
   * @param predicates
   * @param groupBys
   * @return
   */
  private String buildQuery(String aggregations, String predicates, String groupBys) {
    List<String> components = new ArrayList<>();

    components.add(SELECT);
    components.add(aggregations);
    components.add(FROM);
    components.add(_tableName);
    components.add(predicates);
    components.add(groupBys);

    return StringUtils.join(components, " ");
  }

  /**
   * Randomly generate a query
   * @return Return the generated query
   */
  public String nextQuery() {
    return buildQuery(generateAggregations(), generatePredicates(), generateGroupBys());
  }
}
