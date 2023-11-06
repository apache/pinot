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
package org.apache.pinot.common.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SqlResultComparator {
  private SqlResultComparator() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlResultComparator.class);

  private static final String FIELD_RESULT_TABLE = "resultTable";
  private static final String FIELD_DATA_SCHEMA = "dataSchema";
  private static final String FIELD_COLUMN_NAMES = "columnNames";
  private static final String FIELD_COLUMN_DATA_TYPES = "columnDataTypes";
  private static final String FIELD_ROWS = "rows";
  private static final String FIELD_IS_SUPERSET = "isSuperset";
  private static final String FIELD_NUM_DOCS_SCANNED = "numDocsScanned";
  private static final String FIELD_EXCEPTIONS = "exceptions";
  private static final String FIELD_NUM_SERVERS_QUERIED = "numServersQueried";
  private static final String FIELD_NUM_SERVERS_RESPONDED = "numServersResponded";
  private static final String FIELD_NUM_SEGMENTS_QUERIED = "numSegmentsQueried";
  private static final String FIELD_NUM_SEGMENTS_PROCESSED = "numSegmentsProcessed";
  private static final String FIELD_NUM_SEGMENTS_MATCHED = "numSegmentsMatched";
  private static final String FIELD_NUM_CONSUMING_SEGMENTS_QUERIED = "numConsumingSegmentsQueried";
  private static final String FIELD_NUM_ENTRIES_SCANNED_IN_FILTER = "numEntriesScannedInFilter";
  private static final String FIELD_NUM_ENTRIES_SCANNED_POST_FILTER = "numEntriesScannedPostFilter";
  private static final String FIELD_NUM_GROUPS_LIMIT_REACHED = "numGroupsLimitReached";
  private static final String FIELD_IS_ACCURATE_GROUP_BY = "isAccurateGroupBy";

  private static final String FIELD_TYPE_INT = "INT";
  private static final String FIELD_TYPE_LONG = "LONG";
  private static final String FIELD_TYPE_FLOAT = "FLOAT";
  private static final String FIELD_TYPE_DOUBLE = "DOUBLE";
  private static final String FIELD_TYPE_STRING = "STRING";
  private static final String FIELD_TYPE_BYTES = "BYTES";
  private static final String FIELD_TYPE_INT_ARRAY = "INT_ARRAY";
  private static final String FIELD_TYPE_LONG_ARRAY = "LONG_ARRAY";
  private static final String FIELD_TYPE_FLOAT_ARRAY = "FLOAT_ARRAY";
  private static final String FIELD_TYPE_DOUBLE_ARRAY = "DOUBLE_ARRAY";
  private static final String FIELD_TYPE_STRING_ARRAY = "STRING_ARRAY";
  private static final String FIELD_TYPE_BYTES_ARRRAY = "BYTES_ARRRAY";

  private static final SqlParser.Config SQL_PARSER_CONFIG =
      SqlParser.configBuilder().setLex(Lex.MYSQL_ANSI).setConformance(SqlConformanceEnum.BABEL)
          .setParserFactory(SqlBabelParserImpl.FACTORY).build();

  public static boolean areEqual(JsonNode actual, JsonNode expected, String query)
      throws IOException {
    if (hasExceptions(actual)) {
      return false;
    }

    if (areEmpty(actual, expected)) {
      return true;
    }

    if (!areDataSchemaEqual(actual, expected)) {
      return false;
    }

    ArrayNode actualRows = (ArrayNode) actual.get(FIELD_RESULT_TABLE).get(FIELD_ROWS);
    ArrayNode expectedRows = (ArrayNode) expected.get(FIELD_RESULT_TABLE).get(FIELD_ROWS);
    ArrayNode columnDataTypes = (ArrayNode) expected.get(FIELD_RESULT_TABLE).get(FIELD_DATA_SCHEMA).
        get(FIELD_COLUMN_DATA_TYPES);

    convertNumbersToString(expectedRows, columnDataTypes);
    convertNumbersToString(actualRows, columnDataTypes);

    List<String> actualElementsSerialized = new ArrayList<>();
    List<String> expectedElementsSerialized = new ArrayList<>();
    for (int i = 0; i < actualRows.size(); i++) {
      actualElementsSerialized.add(actualRows.get(i).toString());
    }
    for (int i = 0; i < expectedRows.size(); i++) {
      expectedElementsSerialized.add(expectedRows.get(i).toString());
    }
    /*
     * If the test compares the returned results to be a subset of total qualified results, ignore the comparison of
     * result length and metadata(numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, etc).
     */
    if (expected.has(FIELD_IS_SUPERSET) && expected.get(FIELD_IS_SUPERSET).asBoolean(false)) {
      return areElementsSubset(actualElementsSerialized, expectedElementsSerialized);
    }
    if (!areLengthsEqual(actual, expected)) {
      return false;
    }
    boolean areResultsEqual = isOrderByQuery(query) ? areOrderByQueryElementsEqual(actualRows, expectedRows,
        actualElementsSerialized, expectedElementsSerialized, query)
        : areNonOrderByQueryElementsEqual(actualElementsSerialized, expectedElementsSerialized);
    /*
     * Pinot servers implement early termination optimization (process in parallel and early return if we get enough
     * documents to fulfill the LIMIT and OFFSET requirement) for queries for the following cases:
     * - selection without order by
     * - selection with order by (by sorting the segments on min-max value)
     * - DISTINCT queries.
     * numDocsScanned is non-deterministic for those queries so numDocsScanned comparison should be skipped.
     *
     * In other cases, we accept if numDocsScanned is better than the expected one, since that is indicative of
     * performance improvement.
     *
     * NOTE: DISTINCT queries are modeled as non-selection queries during query processing, but all DISTINCT
     * queries are selection queries during Calcite parsing (DISTINCT queries are selection queries with
     * selectNode.getModifierNode(SqlSelectKeyword.DISTINCT) != null).
     */
    if (areResultsEqual) {
      // Results are good, check for any metadata differences here.
      if (!isSelectionQuery(query) && !isNumDocsScannedBetter(actual, expected)) {
        return false;
      }
      if (isNumEntriesScannedInFilterPresent(expected) && !isNumEntriesScannedInFilterBetter(actual, expected)) {
        return false;
      }
      if (isNumConsumingSegmentsQueriedPresent(expected) && !areNumConsumingSegmentsQueriedEqual(actual, expected)) {
        return false;
      }
    }
    return areResultsEqual;
  }

  private static boolean areOrderByQueryElementsEqual(ArrayNode actualElements, ArrayNode expectedElements,
      List<String> actualElementsSerialized, List<String> expectedElementsSerialized, String query) {
    // Happy path, the results match exactly.
    if (actualElementsSerialized.equals(expectedElementsSerialized)) {
      LOGGER.debug("The results of the ordered query match exactly!");
      return true;
    }

    /*
     * Unhappy path, it possible that the returned results:
     * - are not ordered by all columns.
     * - to be a subset of total qualified results.
     * In this case, we divide the results into groups (based on the ordered by column values), then compare the actual
     * results and expected results group by group:
     * - ordered by column values should be the same.
     * - other column values (columns not in the order-by list) should be in the same set.
     * - for the last group, since it's possible that the returned results to be a subset of total qualified results,
     *   skipping the value comparison for other columns.
     *
     * Let's say we have a table:
     * column_name: A, B, C
     * row 0 value: 1, 2, 3
     * row 1 value: 2, 2, 4
     * row 2 value: 3, 7, 5
     * row 3 value: 3, 2, 3
     * row 4 value: 4, 2, 5
     * row 5 value: 4, 7, 6
     *
     * There are 4 possible result for query `SELECT * from table ordered by A LIMIT 5`:
     * 1) [row 0, row 1, row 2, row 3, row 4]
     * 2) [row 0, row 1, row 3, row 2, row 4]
     * 3) [row 0, row 1, row 2, row 3, row 5]
     * 4) [row 0, row 1, row 3, row 2, row 5]
     *
     * So we will divide the result into 4 groups (based on value of A):
     * group 1: [row 0]
     * group 2: [row 1]
     * group 3: [row 2, row 3], or [row 3, row 2]
     * group 4: [row 4] or [row 5]
     *
     * During comparison:
     * - for group 1, 2, 3: value of A should be the same, value of (B, C) should be in the same set.
     * - for group 4: only verify the value of A should be the same.
     */

    List<Integer> orderByColumnIndexs = getOrderByColumnIndexs(query);
    LinkedHashMap<String, List<String>> actualOrderByColumnValuesToOtherColumnValuesMap = new LinkedHashMap<>();
    LinkedHashMap<String, List<String>> expectedOrderByColumnValuesToOtherColumnValuesMap = new LinkedHashMap<>();
    String lastGroupOrderByColumnValues = "";
    for (int i = 0; i < actualElements.size(); i++) {
      String actualOrderByColumnValues = "";
      String expectedOrderByColumnValues = "";
      String actualOtherColumnValues = "";
      String expectOtherColumnValues = "";
      ArrayNode actualValue = (ArrayNode) actualElements.get(i);
      ArrayNode expectedValue = (ArrayNode) expectedElements.get(i);

      for (int j = 0; j < actualValue.size(); j++) {
        if (orderByColumnIndexs.contains(j)) {
          actualOrderByColumnValues += ", " + actualValue.get(j).toString();
          expectedOrderByColumnValues += ", " + expectedValue.get(j).toString();
        } else {
          actualOtherColumnValues += ", " + actualValue.get(j).toString();
          expectOtherColumnValues += ", " + expectedValue.get(j).toString();
        }
      }
      lastGroupOrderByColumnValues = actualOrderByColumnValues;

      actualOrderByColumnValuesToOtherColumnValuesMap.
          computeIfAbsent(actualOrderByColumnValues, k -> new LinkedList<>()).
          add(actualOtherColumnValues);
      expectedOrderByColumnValuesToOtherColumnValuesMap.
          computeIfAbsent(expectedOrderByColumnValues, k -> new LinkedList<>()).
          add(expectOtherColumnValues);
    }

    if (!actualOrderByColumnValuesToOtherColumnValuesMap.keySet().
        equals(expectedOrderByColumnValuesToOtherColumnValuesMap.keySet())) {
      LOGGER.error("The results of the ordered query has different groups, actual: {}, expected: {}",
          actualOrderByColumnValuesToOtherColumnValuesMap.keySet(),
          expectedOrderByColumnValuesToOtherColumnValuesMap.keySet());
      return false;
    }

    for (Map.Entry<String, List<String>> entry : actualOrderByColumnValuesToOtherColumnValuesMap.entrySet()) {
      String orderByColumnValues = entry.getKey();
      // For the last group, skip the value comparison for other columns.
      if (orderByColumnValues.equals(lastGroupOrderByColumnValues)) {
        continue;
      }
      List<String> actualOtherColumnValues = entry.getValue();
      List<String> expectedOtherColumnValues =
          expectedOrderByColumnValuesToOtherColumnValuesMap.get(orderByColumnValues);
      Collections.sort(actualOtherColumnValues);
      Collections.sort(expectedOtherColumnValues);
      if (!actualOtherColumnValues.equals(expectedOtherColumnValues)) {
        LOGGER.error(
            "The results of the ordered query has different non-order-by column values for group: {}, actual: {}, "
                + "expected: {}",
            orderByColumnValues, actualOtherColumnValues, expectedOtherColumnValues);
        return false;
      }
    }

    return true;
  }

  /*
   * For non-order-by queries, the result file should specify the isSuperset flag. If that is done, we will never hit
   * this case. If you see any failures, please update the result file.
   */
  private static boolean areNonOrderByQueryElementsEqual(List<String> actualElementsSerialized,
      List<String> expectedElementsSerialized) {
    // sort elements
    actualElementsSerialized.sort(null);
    expectedElementsSerialized.sort(null);
    if (!actualElementsSerialized.equals(expectedElementsSerialized)) {
      LOGGER.error("The results of the non-ordered query don't match. Sorted-expected: '{}', sorted-actual: '{}'",
          expectedElementsSerialized, actualElementsSerialized);
      return false;
    }
    return true;
  }

  private static boolean hasExceptions(JsonNode actual) {
    if (!actual.get(FIELD_EXCEPTIONS).isEmpty()) {
      LOGGER.error("Got exception: {} when querying!", actual.get(FIELD_EXCEPTIONS));
      return true;
    }
    return false;
  }

  public static boolean areMetadataEqual(JsonNode actual, JsonNode expected) {
    /*
     * Since we add more and more several segments with different generations during compatibility test,
     * metadata such as "numSegmentsQueried", "numDocsScanned" will be different, we only compare
     * "numServersQueried" and "numServersResponded" here.
     * */
    return areNumServersQueriedEqual(actual, expected) && areNumServersRespondedEqual(actual, expected);
  }

  private static boolean areNumGroupsLimitReachedEqual(JsonNode actual, JsonNode expected) {
    boolean actualNumGroupsLimitReached = actual.get(FIELD_NUM_GROUPS_LIMIT_REACHED).asBoolean();
    boolean expectedNumGroupsLimitReached = expected.get(FIELD_NUM_GROUPS_LIMIT_REACHED).asBoolean();
    if (actualNumGroupsLimitReached != expectedNumGroupsLimitReached) {
      LOGGER.error("The numGroupsLimitReached don't match! Actual: {}, Expected: {}", actualNumGroupsLimitReached,
          expectedNumGroupsLimitReached);
      return false;
    }
    return true;
  }

  private static boolean isAccurateGroupByEqual(JsonNode actual, JsonNode expected) {
    boolean actualIsAccurateGroupBy = actual.get(FIELD_IS_ACCURATE_GROUP_BY).asBoolean();
    boolean expectedIsAccurateGroupBy = expected.get(FIELD_IS_ACCURATE_GROUP_BY).asBoolean();
    if (actualIsAccurateGroupBy != expectedIsAccurateGroupBy) {
      LOGGER.error("The isAccurateGroupBy field doesn't match! Actual: {}, Expected: {}", actualIsAccurateGroupBy,
          expectedIsAccurateGroupBy);
      return false;
    }
    return true;
  }

  private static boolean isNumConsumingSegmentsQueriedPresent(JsonNode expected) {
    return expected.get(FIELD_NUM_CONSUMING_SEGMENTS_QUERIED) != null;
  }

  private static boolean areNumConsumingSegmentsQueriedEqual(JsonNode actual, JsonNode expected) {
    long actualNumConsumingSegmentsQueried = actual.get(FIELD_NUM_CONSUMING_SEGMENTS_QUERIED).asLong();
    long expectedNumConsumingSegmentsQueried = expected.get(FIELD_NUM_CONSUMING_SEGMENTS_QUERIED).asLong();
    if (actualNumConsumingSegmentsQueried != expectedNumConsumingSegmentsQueried) {
      LOGGER.error("The numConsumingSegmentsQueried don't match! Actual: {}, Expected: {}",
          actualNumConsumingSegmentsQueried, expectedNumConsumingSegmentsQueried);
      return false;
    }
    return true;
  }

  private static boolean areNumSegmentsProcessedEqual(JsonNode actual, JsonNode expected) {
    long actualNumSegmentsProcessed = actual.get(FIELD_NUM_SEGMENTS_PROCESSED).asLong();
    long expectedNumSegmentsProcessed = expected.get(FIELD_NUM_SEGMENTS_PROCESSED).asLong();
    if (actualNumSegmentsProcessed != expectedNumSegmentsProcessed) {
      LOGGER.error("The numSegmentsProcessed don't match! Actual: {}, Expected: {}", actualNumSegmentsProcessed,
          expectedNumSegmentsProcessed);
      return false;
    }
    return true;
  }

  private static boolean areNumSegmentsQueriedEqual(JsonNode actual, JsonNode expected) {
    long actualNumSegmentsQueried = actual.get(FIELD_NUM_SEGMENTS_QUERIED).asLong();
    long expectedNumSegmentsQueried = expected.get(FIELD_NUM_SEGMENTS_QUERIED).asLong();
    if (actualNumSegmentsQueried != expectedNumSegmentsQueried) {
      LOGGER.error("The numSegmentsQueried don't match! Actual: {}, Expected: {}", actualNumSegmentsQueried,
          expectedNumSegmentsQueried);
      return false;
    }
    return true;
  }

  private static boolean areNumSegmentsMatchedEqual(JsonNode actual, JsonNode expected) {
    long actualNumSegmentsMatched = actual.get(FIELD_NUM_SEGMENTS_MATCHED).asLong();
    long expectedNumSegmentsMatched = expected.get(FIELD_NUM_SEGMENTS_MATCHED).asLong();
    if (actualNumSegmentsMatched != expectedNumSegmentsMatched) {
      LOGGER.error("The numSegmentsMatched don't match! Actual: {}, Expected: {}", actualNumSegmentsMatched,
          expectedNumSegmentsMatched);
      return false;
    }
    return true;
  }

  private static boolean areNumServersRespondedEqual(JsonNode actual, JsonNode expected) {
    long actualNumServersResponded = actual.get(FIELD_NUM_SERVERS_RESPONDED).asLong();
    long expectedNumServersResponded = expected.get(FIELD_NUM_SERVERS_RESPONDED).asLong();
    if (actualNumServersResponded != expectedNumServersResponded) {
      LOGGER.error("The numServersResponded don't match! Actual: {}, Expected: {}", actualNumServersResponded,
          expectedNumServersResponded);
      return false;
    }
    return true;
  }

  private static boolean areNumServersQueriedEqual(JsonNode actual, JsonNode expected) {
    long actualNumServersQueried = actual.get(FIELD_NUM_SERVERS_QUERIED).asLong();
    long expectedNumServersQueried = expected.get(FIELD_NUM_SERVERS_QUERIED).asLong();
    if (actualNumServersQueried != expectedNumServersQueried) {
      LOGGER.error("The numServersQueried don't match! Actual: {}, Expected: {}", actualNumServersQueried,
          expectedNumServersQueried);
      return false;
    }
    return true;
  }

  private static boolean isNumEntriesScannedInFilterPresent(JsonNode expected) {
    return expected.get(FIELD_NUM_ENTRIES_SCANNED_IN_FILTER) != null;
  }

  private static boolean isNumEntriesScannedInFilterBetter(JsonNode actual, JsonNode expected) {
    long actualNumEntriesScannedInFilter = actual.get(FIELD_NUM_ENTRIES_SCANNED_IN_FILTER).asLong();
    long expectedNumEntriesScannedInFilter = expected.get(FIELD_NUM_ENTRIES_SCANNED_IN_FILTER).asLong();
    if (actualNumEntriesScannedInFilter > expectedNumEntriesScannedInFilter) {
      LOGGER
          .error("The numEntriesScannedInFilter is worse. Actual: {}, Expected: {}", actualNumEntriesScannedInFilter,
              expectedNumEntriesScannedInFilter);
      return false;
    }
    return true;
  }

  private static boolean isNumEntriesScannedPostFilterBetter(JsonNode actual, JsonNode expected) {
    long actualNumEntriesScannedPostFilter = actual.get(FIELD_NUM_ENTRIES_SCANNED_POST_FILTER).asLong();
    long expectedNumEntriesScannedPostFilter = expected.get(FIELD_NUM_ENTRIES_SCANNED_POST_FILTER).asLong();
    if (actualNumEntriesScannedPostFilter > expectedNumEntriesScannedPostFilter) {
      LOGGER.error("The numEntriesScannedPostFilter is worse. Actual: {}, Expected: {}",
          actualNumEntriesScannedPostFilter, expectedNumEntriesScannedPostFilter);
      return false;
    }
    return true;
  }

  private static boolean isNumDocsScannedBetter(JsonNode actual, JsonNode expected) {
    int actualNumDocsScanned = actual.get(FIELD_NUM_DOCS_SCANNED).asInt();
    int expectedNumDocsScanned = expected.get(FIELD_NUM_DOCS_SCANNED).asInt();
    if (actualNumDocsScanned > expectedNumDocsScanned) {
      LOGGER.error("The numDocsScanned is worse. Actual: {}, Expected: {}", actualNumDocsScanned,
          expectedNumDocsScanned);
      return false;
    }
    return true;
  }

  private static boolean areEmpty(JsonNode actual, JsonNode expected) {
    if (isEmpty(actual) && isEmpty(expected)) {
      LOGGER.debug("Empty results, nothing to compare.");
      return true;
    }
    return false;
  }

  public static boolean isEmpty(JsonNode response) {
    int numDocsScanned = response.get(FIELD_NUM_DOCS_SCANNED).asInt();
    return numDocsScanned == 0 || !response.has(FIELD_RESULT_TABLE)
        || response.get(FIELD_RESULT_TABLE).get(FIELD_ROWS).isEmpty();
  }

  private static boolean areLengthsEqual(JsonNode actual, JsonNode expected) {
    int actualLength = actual.get(FIELD_RESULT_TABLE).get(FIELD_ROWS).size();
    int expectedLength = expected.get(FIELD_RESULT_TABLE).get(FIELD_ROWS).size();
    if (actualLength != expectedLength) {
      LOGGER.error("The length of results don't match! Actual: {}, Expected: {}", actualLength, expectedLength);
      return false;
    }
    return true;
  }

  private static boolean areDataSchemaEqual(JsonNode actual, JsonNode expected) {
    /*
     * Field "dataSchema" is an array, which contains "columnNames" and "columnDataTypes". However there is no orders
     * between "columnNames" and "columnDataTypes", so we extract and append them when compare instead of compare
     * "dataSchema" directly.
     */
    JsonNode actualColumnNames = actual.get(FIELD_RESULT_TABLE).get(FIELD_DATA_SCHEMA).get(FIELD_COLUMN_NAMES);
    JsonNode expectedColumnNames = expected.get(FIELD_RESULT_TABLE).get(FIELD_DATA_SCHEMA).get(FIELD_COLUMN_NAMES);
    JsonNode actualColumnDataTypes = actual.get(FIELD_RESULT_TABLE).get(FIELD_DATA_SCHEMA).get(FIELD_COLUMN_DATA_TYPES);
    JsonNode expectedColumnDataTypes =
        expected.get(FIELD_RESULT_TABLE).get(FIELD_DATA_SCHEMA).get(FIELD_COLUMN_DATA_TYPES);

    String actualDataSchemaStr = actualColumnNames.toString() + actualColumnDataTypes.toString();
    String expectedDataSchemaStr = expectedColumnNames.toString() + expectedColumnDataTypes.toString();
    if (!actualDataSchemaStr.equals(expectedDataSchemaStr)) {
      LOGGER.error("The dataSchema don't match! Actual: {}, Expected: {}", actualDataSchemaStr, expectedDataSchemaStr);
      return false;
    }
    return true;
  }

  private static boolean areElementsSubset(List<String> actualElementsSerialized,
      List<String> expectedElementsSerialized) {
    boolean result = expectedElementsSerialized.containsAll(actualElementsSerialized);
    if (!result) {
      LOGGER.error("Actual result '{}' is not a subset of '{}'", actualElementsSerialized, expectedElementsSerialized);
    }
    return result;
  }

  private static void convertNumbersToString(ArrayNode rows, ArrayNode columnDataTypes)
      throws IOException {
    for (int i = 0; i < rows.size(); i++) {
      for (int j = 0; j < columnDataTypes.size(); j++) {
        ArrayNode row = (ArrayNode) rows.get(i);
        String type = columnDataTypes.get(j).asText();
        if (type.equals(FIELD_TYPE_FLOAT) || type.equals(FIELD_TYPE_DOUBLE)) {
          double round = Math.round(Double.valueOf(row.get(j).asText()) * 100) / 100.0;
          String str = String.valueOf(round);
          row.set(j, JsonUtils.stringToJsonNode(str));
        } else if (type.equals(FIELD_TYPE_FLOAT_ARRAY) || type.equals(FIELD_TYPE_DOUBLE_ARRAY)) {
          ArrayNode jsonArray = (ArrayNode) rows.get(i).get(j);
          List<String> arrayStr = new ArrayList<>();
          for (int k = 0; k < jsonArray.size(); k++) {
            double round = Math.round(Double.valueOf(jsonArray.get(k).asText()) * 100) / 100;
            arrayStr.add(String.valueOf(round));
          }
          row.set(j, JsonUtils.stringToJsonNode(arrayStr.toString()));
        }
      }
    }
  }

  private static boolean isOrderByQuery(String query) {
    SqlParser sqlParser = SqlParser.create(query, SQL_PARSER_CONFIG);
    try {
      SqlNode sqlNode = sqlParser.parseQuery();
      boolean isOrderBy = sqlNode.getKind() == SqlKind.ORDER_BY;
      if (!isOrderBy) {
        return false;
      }
      SqlOrderBy sqlOrderBy = (SqlOrderBy) sqlNode;
      SqlNodeList orderByColumns = sqlOrderBy.orderList;
      return orderByColumns != null && orderByColumns.size() != 0;
    } catch (SqlParseException e) {
      throw new RuntimeException("Cannot parse query: " + query, e);
    }
  }

  // Selection query is the one that doesn't have group by or aggregation functions.
  private static boolean isSelectionQuery(String query) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    if (pinotQuery.getSelectList() == null) {
      return false;
    }
    if (pinotQuery.isSetGroupByList()) {
      return false;
    }
    for (Expression expression : pinotQuery.getSelectList()) {
      if (expression.getType() == ExpressionType.FUNCTION) {
        Function functionCall = expression.getFunctionCall();
        String functionName = functionCall.getOperator();
        if (AggregationFunctionType.isAggregationFunction(functionName)) {
          return false;
        }
      }
    }
    return true;
  }

  private static List<Integer> getOrderByColumnIndexs(String query) {
    SqlSelect selectNodeWithOrderBy = getSelectNodeWithOrderBy(query);
    SqlNodeList selectList = selectNodeWithOrderBy.getSelectList();
    List<String> selectColumnNames = new ArrayList<>();
    for (SqlNode node : selectList) {
      selectColumnNames.add(node.toString());
    }
    SqlNodeList orderList = selectNodeWithOrderBy.getOrderList();
    List<String> orderByColumnNames = new ArrayList<>();
    for (SqlNode node : orderList) {
      String columnName =
          node instanceof SqlCall ? ((SqlCall) node).getOperandList().get(0).toString() : node.toString();
      orderByColumnNames.add(columnName);
    }
    List<Integer> orderByColumnIndexs = new LinkedList<>();
    int i = 0;
    int j = 0;
    while (i < orderByColumnNames.size()) {
      while (j < selectColumnNames.size() && !selectColumnNames.get(j).equals(orderByColumnNames.get(i))) {
        j++;
      }
      orderByColumnIndexs.add(j);
      i++;
    }
    return orderByColumnIndexs;
  }

  private static SqlSelect getSelectNodeWithOrderBy(String query) {
    SqlParser sqlParser = SqlParser.create(query, SQL_PARSER_CONFIG);
    try {
      SqlNode sqlNode = sqlParser.parseQuery();
      SqlSelect selectNode;
      if (sqlNode instanceof SqlOrderBy) {
        // Store order-by info into the select sql node
        SqlOrderBy orderByNode = (SqlOrderBy) sqlNode;
        selectNode = (SqlSelect) orderByNode.query;
        selectNode.setOrderBy(orderByNode.orderList);
        selectNode.setFetch(orderByNode.fetch);
        selectNode.setOffset(orderByNode.offset);
      } else {
        selectNode = (SqlSelect) sqlNode;
      }
      return selectNode;
    } catch (SqlParseException e) {
      throw new RuntimeException("Cannot parse query: " + query, e);
    }
  }
}
