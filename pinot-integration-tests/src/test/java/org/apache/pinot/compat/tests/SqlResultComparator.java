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
package org.apache.pinot.compat.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SqlResultComparator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlResultComparator.class);

  private static final String FIELD_RESULT_TABLE = "resultTable";
  private static final String FIELD_DATA_SCHEMA = "dataSchema";
  private static final String FIELD_ROWS = "rows";
  private static final String FIELD_COLUMN_DATA_TYPES = "columnDataTypes";
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

  private static final SqlParser.Config SQL_PARSER_CONFIG = SqlParser.configBuilder()
      .setLex(Lex.MYSQL_ANSI)
      .setConformance(SqlConformanceEnum.BABEL)
      .setParserFactory(SqlBabelParserImpl.FACTORY)
      .build();

  public static boolean areEqual(JsonNode actual, JsonNode expected, String query) throws IOException {
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
    } else {
      return areLengthsEqual(actual, expected) && areElementsEqual(actualElementsSerialized, expectedElementsSerialized,
          query) && areMetadataEqual(actual, expected);
    }
  }

  public static boolean hasExceptions(JsonNode actual) {
    if (actual.get(FIELD_EXCEPTIONS).size() != 0) {
      LOGGER.error("Got exception: {} when querying!", actual.get(FIELD_EXCEPTIONS));
      return true;
    }
    return false;
  }

  public static boolean areMetadataEqual(JsonNode actual, JsonNode expected) {
    return areNumServersQueriedEqual(actual, expected) && areNumServersRespondedEqual(actual, expected)
        && areNumSegmentsQueriedEqual(actual, expected) && areNumSegmentsProcessedEqual(actual, expected)
        && areNumSegmentsMatchedEqual(actual, expected) && areNumConsumingSegmentsQueriedEqual(actual, expected)
        && areNumDocsScannedEqual(actual, expected) && areNumEntriesScannedInFilterEqual(actual, expected)
        && areNumEntriesScannedPostFilterEqual(actual, expected) && areNumGroupsLimitReachedEqual(actual, expected);
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

  private static boolean areNumEntriesScannedInFilterEqual(JsonNode actual, JsonNode expected) {
    long actualNumEntriesScannedInFilter = actual.get(FIELD_NUM_ENTRIES_SCANNED_IN_FILTER).asLong();
    long expectedNumEntriesScannedInFilter = expected.get(FIELD_NUM_ENTRIES_SCANNED_IN_FILTER).asLong();
    if (actualNumEntriesScannedInFilter != expectedNumEntriesScannedInFilter) {
      LOGGER.error("The numEntriesScannedInFilter don't match! Actual: {}, Expected: {}",
          actualNumEntriesScannedInFilter, expectedNumEntriesScannedInFilter);
      return false;
    }
    return true;
  }

  private static boolean areNumEntriesScannedPostFilterEqual(JsonNode actual, JsonNode expected) {
    long actualNumEntriesScannedPostFilter = actual.get(FIELD_NUM_ENTRIES_SCANNED_POST_FILTER).asLong();
    long expectedNumEntriesScannedPostFilter = expected.get(FIELD_NUM_ENTRIES_SCANNED_POST_FILTER).asLong();
    if (actualNumEntriesScannedPostFilter != expectedNumEntriesScannedPostFilter) {
      LOGGER.error("The numEntriesScannedPostFilter don't match! Actual: {}, Expected: {}",
          actualNumEntriesScannedPostFilter, expectedNumEntriesScannedPostFilter);
      return false;
    }
    return true;
  }

  private static boolean areNumDocsScannedEqual(JsonNode actual, JsonNode expected) {
    int actualNumDocsScanned = actual.get(FIELD_NUM_DOCS_SCANNED).asInt();
    int expectedNumDocsScanned = expected.get(FIELD_NUM_DOCS_SCANNED).asInt();
    if (actualNumDocsScanned != expectedNumDocsScanned) {
      LOGGER.error("The numDocsScanned don't match! Actual: {}, Expected: {}", actualNumDocsScanned,
          expectedNumDocsScanned);
      return false;
    }
    return true;
  }

  private static boolean areEmpty(JsonNode actual, JsonNode expected) {
    int actualNumDocsScanned = actual.get(FIELD_NUM_DOCS_SCANNED).asInt();
    int expectedNumDocsScanned = expected.get(FIELD_NUM_DOCS_SCANNED).asInt();
    if ((actualNumDocsScanned == 0 && expectedNumDocsScanned == 0) || (!actual.has(FIELD_RESULT_TABLE) && !expected.has(
        FIELD_RESULT_TABLE)) || (actual.get(FIELD_RESULT_TABLE).get(FIELD_ROWS).size() == 0
        && expected.get(FIELD_RESULT_TABLE).get(FIELD_ROWS).size() == 0)) {
      LOGGER.debug("Empty results, nothing to compare.");
      return true;
    }
    return false;
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
    JsonNode actualDataSchema = actual.get(FIELD_RESULT_TABLE).get(FIELD_DATA_SCHEMA);
    JsonNode expecteDataSchema = expected.get(FIELD_RESULT_TABLE).get(FIELD_DATA_SCHEMA);

    String actualDataSchemaStr = actualDataSchema.toString();
    String expecteDataSchemaStr = expecteDataSchema.toString();
    if (!actualDataSchemaStr.equals(expecteDataSchemaStr)) {
      LOGGER.error("The dataSchema don't match! Actual: {}, Expected: {}", actualDataSchema, expecteDataSchema);
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

  private static boolean areElementsEqual(List<String> actualElementsSerialized,
      List<String> expectedElementsSerialized, String query) {
    if (isOrdered(query)) {
      if (!actualElementsSerialized.equals(expectedElementsSerialized)) {
        LOGGER.error("The results of the ordered query don't match! Actual: {}, Expected: {}", actualElementsSerialized,
            expectedElementsSerialized);
        return false;
      }
      return true;
    }

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

  private static void convertNumbersToString(ArrayNode rows, ArrayNode columnDataTypes) throws IOException {
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

  private static boolean isOrdered(String query) {
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
}
