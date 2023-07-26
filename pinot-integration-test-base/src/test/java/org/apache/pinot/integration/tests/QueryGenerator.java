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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * The <code>QueryGenerator</code> class is used to generate random equivalent Pinot and H2 query pairs based on Avro
 * files.
 * <ul>
 *   <li>
 *     Supports COMPARISON, IN and BETWEEN predicate for both single-value and multi-value columns.
 *     <ul>
 *       <li>For multi-value columns, does not support NOT EQUAL and NOT IN.</li>
 *       <li>The reason for this restriction is that number of elements in multi-value columns is not fixed.</li>
 *     </ul>
 *   </li>
 *   <li>Supports single-value data type: BOOLEAN, INT, LONG, FLOAT, DOUBLE, STRING.</li>
 *   <li>Supports multi-value data type: INT, LONG, FLOAT, DOUBLE, STRING.</li>
 *   <li>
 *     Supports aggregation function: SUM, MIN, MAX, AVG, COUNT, DISTINCTCOUNT.
 *     <ul>
 *       <li>SUM, MIN, MAX, AVG can only work on numeric single-value columns.</li>
 *       <li>COUNT, DISTINCTCOUNT can work on any single-value columns.</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class QueryGenerator {
  // Configurable variables.
  private static final int MAX_NUM_SELECTION_COLUMNS = 3;
  private static final int MAX_NUM_AGGREGATION_COLUMNS = 3;
  private static final int MAX_NUM_ORDER_BY_COLUMNS = 3;
  private static final int MAX_NUM_GROUP_BY_COLUMNS = 3;
  private static final int MAX_NUM_PREDICATES = 3;
  private static final int MAX_NUM_IN_CLAUSE_VALUES = 5;
  private static final int MAX_RESULT_LIMIT = 30;
  private static final int MAX_INT_FOR_HAVING_CLAUSE_PREDICATE = 1000;
  private static final float MAX_FLOAT_FOR_HAVING_CLAUSE_PREDICATE = 10000;
  private static final List<String> BOOLEAN_OPERATORS = Arrays.asList("OR", "AND");
  private static final List<String> COMPARISON_OPERATORS = Arrays.asList("=", "<>", "<", ">", "<=", ">=");

  private static final List<String> AGGREGATION_FUNCTIONS =
      Arrays.asList("SUM", "MIN", "MAX", "AVG", "COUNT", "DISTINCTCOUNT");
  private static final List<String> MULTI_STAGE_AGGREGATION_FUNCTIONS =
      Arrays.asList("SUM", "MIN", "MAX", "AVG", "COUNT");
  private static final Random RANDOM = new Random();

  private final Map<String, Set<String>> _columnToValueSet = new HashMap<>();
  private final Map<String, List<String>> _columnToValueList = new HashMap<>();
  private final List<String> _columnNames = new ArrayList<>();
  private final List<String> _singleValueColumnNames = new ArrayList<>();
  private final List<String> _singleValueNumericalColumnNames = new ArrayList<>();
  private final Map<String, Integer> _multiValueColumnMaxNumElements = new HashMap<>();

  private final List<QueryGenerationStrategy> _queryGenerationStrategies =
      Arrays.asList(new SelectionQueryGenerationStrategy(), new AggregationQueryGenerationStrategy());
  private final List<PredicateGenerator> _singleValuePredicateGenerators =
      Arrays.asList(new SingleValueComparisonPredicateGenerator(), new SingleValueInPredicateGenerator(),
          new SingleValueBetweenPredicateGenerator(), new SingleValueRegexPredicateGenerator());
  private final List<PredicateGenerator> _multistageSingleValuePredicateGenerators =
      Arrays.asList(new SingleValueComparisonPredicateGenerator(), new SingleValueInPredicateGenerator(),
          new SingleValueBetweenPredicateGenerator());
  private final List<PredicateGenerator> _multiValuePredicateGenerators =
      Arrays.asList(new MultiValueComparisonPredicateGenerator(), new MultiValueInPredicateGenerator(),
          new MultiValueBetweenPredicateGenerator());

  private final String _pinotTableName;
  private final String _h2TableName;
  private boolean _skipMultiValuePredicates = false;
  private boolean _useMultistageEngine = false;

  /**
   * Constructor for <code>QueryGenerator</code>.
   *
   * @param avroFiles list of Avro files.
   * @param pinotTableName Pinot table name.
   * @param h2TableName H2 table name.
   */
  public QueryGenerator(List<File> avroFiles, String pinotTableName, String h2TableName) {
    _pinotTableName = pinotTableName;
    _h2TableName = h2TableName;

    // Read Avro schema and initialize storage.
    File schemaAvroFile = avroFiles.get(0);
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    try (DataFileReader<GenericRecord> fileReader = new DataFileReader<>(schemaAvroFile, datumReader)) {
      Schema schema = fileReader.getSchema();
      for (Schema.Field field : schema.getFields()) {
        String fieldName = field.name();
        Schema fieldSchema = field.schema();
        Schema.Type fieldType = fieldSchema.getType();

        switch (fieldType) {
          case UNION:
            _columnNames.add(fieldName);
            _columnToValueSet.put(fieldName, new HashSet<>());
            Schema.Type type = fieldSchema.getTypes().get(0).getType();
            if (type == Schema.Type.ARRAY) {
              _multiValueColumnMaxNumElements.put(fieldName, 0);
            } else {
              _singleValueColumnNames.add(fieldName);
              if (type != Schema.Type.STRING && type != Schema.Type.BOOLEAN) {
                _singleValueNumericalColumnNames.add(fieldName);
              }
            }
            break;
          case ARRAY:
            _columnNames.add(fieldName);
            _columnToValueSet.put(fieldName, new HashSet<>());
            _multiValueColumnMaxNumElements.put(fieldName, 0);
            break;
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
            _columnNames.add(fieldName);
            _columnToValueSet.put(fieldName, new HashSet<>());
            _singleValueColumnNames.add(fieldName);
            _singleValueNumericalColumnNames.add(fieldName);
            break;
          case BOOLEAN:
          case STRING:
            _columnNames.add(fieldName);
            _columnToValueSet.put(fieldName, new HashSet<>());
            _singleValueColumnNames.add(fieldName);
            break;
          default:
            break;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Load Avro data into storage.
    for (File avroFile : avroFiles) {
      addAvroData(avroFile);
    }

    // Ignore multi-value columns with too many elements.
    prepareToGenerateQueries();
  }

  /**
   * Helper method to store an Avro value into the valid SQL String value set.
   *
   * @param valueSet value set.
   * @param avroValue Avro value.
   */
  private static void storeAvroValueIntoValueSet(Set<String> valueSet, Object avroValue) {
    if (avroValue instanceof Number) {
      // For Number object, store raw value.
      valueSet.add(avroValue.toString());
    } else {
      // For non-Number object, escape single quote.
      valueSet.add("'" + avroValue.toString().replace("'", "''") + "'");
    }
  }

  /**
   * Helper method to join several {@link String} elements with ' '.
   *
   * @param elements elements to be joined.
   * @return joined result.
   */
  private static String joinWithSpaces(String... elements) {
    StringBuilder stringBuilder = new StringBuilder();
    for (String element : elements) {
      if (!element.isEmpty()) {
        stringBuilder.append(element).append(' ');
      }
    }
    return stringBuilder.substring(0, stringBuilder.length() - 1);
  }

  /**
   * Sample main class for the query generator.
   *
   * @param args arguments.
   */
  public static void main(String[] args)
      throws Exception {
    File avroFile = new File("pinot-integration-tests/src/test/resources/On_Time_On_Time_Performance_2014_1.avro");
    QueryGenerator queryGenerator = new QueryGenerator(Collections.singletonList(avroFile), "mytable", "mytable");
    File outputFile = new File(
        "pinot-integration-tests/src/test/resources/On_Time_On_Time_Performance_2014_100k_subset.test_queries_10K.sql");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
      for (int i = 0; i < 10000; i++) {
        Query query = queryGenerator.generateQuery();
        ObjectNode queryJson = JsonUtils.newObjectNode();
        queryJson.put("sql", query.generatePinotQuery());
        queryJson.set("hsqls", JsonUtils.objectToJsonNode(query.generateH2Query()));
        writer.write(queryJson.toString());
        writer.newLine();
      }
    }
  }

  /**
   * Helper method to read in an Avro file and add data to the storage.
   *
   * @param avroFile Avro file.
   */
  private void addAvroData(File avroFile) {
    // Read in records and update the values stored.
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    try (DataFileReader<GenericRecord> fileReader = new DataFileReader<>(avroFile, datumReader)) {
      for (GenericRecord genericRecord : fileReader) {
        for (String columnName : _columnNames) {
          Set<String> values = _columnToValueSet.get(columnName);

          // Turn the Avro value into a valid SQL String token.
          Object avroValue = genericRecord.get(columnName);
          if (avroValue != null) {
            Integer storedMaxNumElements = _multiValueColumnMaxNumElements.get(columnName);
            if (storedMaxNumElements != null) {
              // Multi-value column
              GenericData.Array array = (GenericData.Array) avroValue;
              int numElements = array.size();
              if (storedMaxNumElements < numElements) {
                _multiValueColumnMaxNumElements.put(columnName, numElements);
              }
              for (Object element : array) {
                storeAvroValueIntoValueSet(values, element);
              }
            } else {
              // Single-value column
              storeAvroValueIntoValueSet(values, avroValue);
            }
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Helper method to finish initialization of the <code>QueryGenerator</code>, removing multi-value columns with too
   * many elements and dumping storage into the final map from column name to list of column values.
   * <p>Called after all Avro data loaded.
   */
  private void prepareToGenerateQueries() {
    Iterator<String> columnNameIterator = _columnNames.iterator();
    while (columnNameIterator.hasNext()) {
      String columnName = columnNameIterator.next();

      // Remove multi-value columns with more than MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE elements.
      Integer maxNumElements = _multiValueColumnMaxNumElements.get(columnName);
      if (maxNumElements != null
          && maxNumElements > ClusterIntegrationTestUtils.MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE) {
        columnNameIterator.remove();
        _multiValueColumnMaxNumElements.remove(columnName);
      } else {
        _columnToValueList.put(columnName, new ArrayList<>(_columnToValueSet.get(columnName)));
      }
    }

    // Free the other copy of the data.
    _columnToValueSet.clear();
  }

  /**
   * Set whether to skip predicates on multi-value columns.
   *
   * @param skipMultiValuePredicates whether to skip predicates on multi-value columns.
   */
  public void setSkipMultiValuePredicates(boolean skipMultiValuePredicates) {
    _skipMultiValuePredicates = skipMultiValuePredicates;
  }

  public void setUseMultistageEngine(boolean useMultistageEngine) {
    _useMultistageEngine = useMultistageEngine;
  }

  /**
   * Helper method to pick a random value from the values list.
   *
   * @param list values list.
   * @param <T> type of the value.
   * @return randomly picked value.
   */
  private <T> T pickRandom(List<T> list) {
    return list.get(RANDOM.nextInt(list.size()));
  }

  private List<PredicateGenerator> getSingleValuePredicateGenerators() {
    return _useMultistageEngine ? _multistageSingleValuePredicateGenerators : _singleValuePredicateGenerators;
  }

  /**
   * Generate one selection or aggregation query.
   *
   * @return generated query.
   */
  public Query generateQuery() {
    return pickRandom(_queryGenerationStrategies).generateQuery();
  }

  /**
   * Helper method to generate a predicate query fragment.
   *
   * @return generated predicate query fragment.
   */
  private PredicateQueryFragment generatePredicate() {
    // Generate at most MAX_NUM_PREDICATES predicates.
    int predicateCount = RANDOM.nextInt(MAX_NUM_PREDICATES + 1);

    List<QueryFragment> predicates = new ArrayList<>(predicateCount);
    while (predicates.size() < predicateCount) {
      String columnName = pickRandom(_columnNames);
      if (!_columnToValueList.get(columnName).isEmpty()) {
        if (!_multiValueColumnMaxNumElements.containsKey(columnName)) {
          // Single-value column.
          predicates.add(pickRandom(getSingleValuePredicateGenerators()).generatePredicate(columnName,
              _useMultistageEngine));
        } else if (!_skipMultiValuePredicates) {
          // Multi-value column.
          predicates.add(
              pickRandom(_multiValuePredicateGenerators).generatePredicate(columnName, _useMultistageEngine));
        }
      }
    }

    if (predicateCount < 2) {
      // No need to join.
      return new PredicateQueryFragment(predicates, Collections.emptyList());
    } else {
      // Join predicates with ANDs and ORs.
      List<QueryFragment> operators = new ArrayList<>(predicateCount - 1);
      for (int i = 1; i < predicateCount; i++) {
        operators.add(new StringQueryFragment(pickRandom(BOOLEAN_OPERATORS)));
      }
      return new PredicateQueryFragment(predicates, operators);
    }
  }

  public interface Query {

    String generatePinotQuery();

    String generateH2Query();
  }

  private interface QueryFragment {

    String generatePinotQuery();

    String generateH2Query();
  }

  /**
   * Query generation strategy interface with capability of generating query using specific strategy.
   */
  private interface QueryGenerationStrategy {

    /**
     * Generate a query using specific strategy.
     *
     * @return generated query.
     */
    Query generateQuery();
  }

  /**
   * Predicate generator interface with capability of generating a predicate query fragment on a column.
   */
  private interface PredicateGenerator {

    /**
     * Generate a predicate query fragment on a column.
     *
     * @param columnName          column name.
     * @param useMultistageEngine
     * @return generated predicate query fragment.
     */
    QueryFragment generatePredicate(String columnName, boolean useMultistageEngine);
  }

  /**
   * Selection query.
   */
  private class SelectionQuery implements Query {
    final List<String> _projectionColumns;
    final PredicateQueryFragment _predicate;
    final OrderByQueryFragment _orderBy;
    final LimitQueryFragment _limit;

    /**
     * Constructor for <code>SelectionQuery</code>.
     *
     * @param projectionColumns projection columns.
     * @param orderBy order by fragment.
     * @param predicate predicate fragment.
     * @param limit limit fragment.
     */
    public SelectionQuery(List<String> projectionColumns, PredicateQueryFragment predicate,
        OrderByQueryFragment orderBy, LimitQueryFragment limit) {
      _projectionColumns = projectionColumns;
      _orderBy = orderBy;
      _predicate = predicate;
      _limit = limit;
    }

    @Override
    public String generatePinotQuery() {
      return joinWithSpaces("SELECT", StringUtils.join(_projectionColumns, ", "), "FROM", _pinotTableName,
          _predicate.generatePinotQuery(), _orderBy.generatePinotQuery(), _limit.generatePinotQuery());
    }

    @Override
    public String generateH2Query() {
      List<String> h2ProjectionColumns = new ArrayList<>();
      for (String projectionColumn : _projectionColumns) {
        h2ProjectionColumns.add(String.format("`%s`", projectionColumn));
      }
      return joinWithSpaces("SELECT", StringUtils.join(h2ProjectionColumns, ", "), "FROM", _h2TableName,
          _predicate.generateH2Query(), _orderBy.generateH2Query(), _limit.generateH2Query());
    }
  }

  /**
   * Aggregation query.
   */
  private class AggregationQuery implements Query {
    final List<String> _aggregateColumnsAndFunctions;
    final PredicateQueryFragment _predicate;
    final HavingQueryFragment _havingPredicate;
    final Set<String> _groupColumns;
    final LimitQueryFragment _limit;

    /**
     * Constructor for <code>AggregationQuery</code>.
     *
     * @param aggregateColumnsAndFunctions aggregation functions.
     * @param predicate predicate fragment.
     * @param groupColumns group-by columns.
     * @param havingPredicate having predicate fragment.
     * @param limit limit fragment.
     */
    public AggregationQuery(List<String> aggregateColumnsAndFunctions, PredicateQueryFragment predicate,
        Set<String> groupColumns, HavingQueryFragment havingPredicate, LimitQueryFragment limit) {
      _aggregateColumnsAndFunctions = aggregateColumnsAndFunctions;
      _predicate = predicate;
      _groupColumns = groupColumns;
      _havingPredicate = havingPredicate;
      _limit = limit;
    }

    @Override
    public String generatePinotQuery() {
      List<String> pinotAggregateColumnAndFunctions =
          (_useMultistageEngine && !_skipMultiValuePredicates) ? generatePinotMultistageQuery()
              : _aggregateColumnsAndFunctions;
      if (_groupColumns.isEmpty()) {
        return joinWithSpaces("SELECT", StringUtils.join(pinotAggregateColumnAndFunctions, ", "), "FROM",
            _pinotTableName, _predicate.generatePinotQuery());
      } else {
        return joinWithSpaces("SELECT", StringUtils.join(pinotAggregateColumnAndFunctions, ", "), "FROM",
            _pinotTableName, _predicate.generatePinotQuery(), "GROUP BY", StringUtils.join(_groupColumns, ", "),
            _havingPredicate.generatePinotQuery(), _limit.generatePinotQuery());
      }
    }

    public List<String> generatePinotMultistageQuery() {
      List<String> pinotAggregateColumnAndFunctions = new ArrayList<>();
      for (String aggregateColumnAndFunction : _aggregateColumnsAndFunctions) {
        String pinotAggregateFunction = aggregateColumnAndFunction;
        String pinotAggregateColumnAndFunction = pinotAggregateFunction;
        if (!pinotAggregateFunction.equals("COUNT(*)")) {
          pinotAggregateFunction = pinotAggregateFunction.replace("(", "(`").replace(")", "`)");
        }
        if (!pinotAggregateFunction.contains("(")) {
          pinotAggregateFunction = String.format("`%s`", pinotAggregateFunction);
        }
        if (AGGREGATION_FUNCTIONS.contains(pinotAggregateFunction.substring(0, 3))) {
          // For multistage query, we need to explicit hoist the data type to avoid overflow.
          String aggFunctionName = pinotAggregateFunction.substring(0, 3);
          String replacedPinotAggregationFunction =
              pinotAggregateFunction.replace(aggFunctionName + "(", aggFunctionName + "(CAST(");
          if ("SUM".equalsIgnoreCase(aggFunctionName)) {
            pinotAggregateColumnAndFunction = replacedPinotAggregationFunction.replace(")", " AS BIGINT))");
          }
          if ("AVG".equalsIgnoreCase(aggFunctionName)) {
            pinotAggregateColumnAndFunction = replacedPinotAggregationFunction.replace(")", " AS DOUBLE))");
          }
        }
        pinotAggregateColumnAndFunctions.add(pinotAggregateColumnAndFunction);
      }
      return pinotAggregateColumnAndFunctions;
    }

    @Override
    public String generateH2Query() {
      List<String> h2AggregateColumnAndFunctions = new ArrayList<>();
      for (String aggregateColumnAndFunction : _aggregateColumnsAndFunctions) {
        String h2AggregateColumnAndFunction;
        String pinotAggregateFunction = aggregateColumnAndFunction;
        if (!pinotAggregateFunction.equals("COUNT(*)")) {
          pinotAggregateFunction = pinotAggregateFunction.replace("(", "(`").replace(")", "`)");
        }
        if (!pinotAggregateFunction.contains("(")) {
          pinotAggregateFunction = String.format("`%s`", pinotAggregateFunction);
        }
        // Make 'AVG' and
        if (pinotAggregateFunction.startsWith("DISTINCTCOUNT(")) {
          // make 'DISTINCTCOUNT(..)' compatible with H2 SQL query using 'COUNT(DISTINCT(..)'
          h2AggregateColumnAndFunction = pinotAggregateFunction.replace("DISTINCTCOUNT(", "COUNT(DISTINCT ");
        } else if (AGGREGATION_FUNCTIONS.contains(pinotAggregateFunction.substring(0, 3))) {
          // make AGG functions (SUM, MIN, MAX, AVG) compatible with H2 SQL query.
          // this is because Pinot queries casts all to double before doing aggregation
          String aggFunctionName = pinotAggregateFunction.substring(0, 3);
          h2AggregateColumnAndFunction = pinotAggregateFunction
              .replace(aggFunctionName + "(", aggFunctionName + "(CAST(")
              .replace(")", " AS DOUBLE))");
        } else {
          h2AggregateColumnAndFunction = pinotAggregateFunction;
        }
        h2AggregateColumnAndFunctions.add(h2AggregateColumnAndFunction);
      }
      if (_groupColumns.isEmpty()) {
        return joinWithSpaces("SELECT", StringUtils.join(h2AggregateColumnAndFunctions, ", "), "FROM", _h2TableName,
            _predicate.generateH2Query());
      } else {
        return joinWithSpaces("SELECT", StringUtils.join(h2AggregateColumnAndFunctions, ", "), "FROM", _h2TableName,
            _predicate.generateH2Query(), "GROUP BY",
            StringUtils.join(_groupColumns.stream().map(c -> String.format("`%s`", c))
                .collect(Collectors.toList()), ", "),
            _havingPredicate.generateH2Query(), _limit.generateH2Query());
      }
    }
  }

  /**
   * Most basic query fragment.
   */
  private static class StringQueryFragment implements QueryFragment {
    final String _pinotQuery;
    final String _h2Query;

    /**
     * Constructor with same Pinot and H2 query fragment.
     */
    StringQueryFragment(String query) {
      _pinotQuery = query;
      _h2Query = query;
    }

    /**
     * Constructor for <code>StringQueryFragment</code> with different Pinot and H2 query fragment.
     */
    StringQueryFragment(String pinotQuery, String h2Query) {
      _pinotQuery = pinotQuery;
      _h2Query = h2Query;
    }

    @Override
    public String generatePinotQuery() {
      return _pinotQuery;
    }

    @Override
    public String generateH2Query() {
      return _h2Query;
    }
  }

  /**
   * Limit query fragment for selection queries.
   * <ul>
   *   <li>SELECT ... FROM ... WHERE ... 'LIMIT ...'</li>
   * </ul>
   */
  private static class LimitQueryFragment extends StringQueryFragment {
    LimitQueryFragment(int limit) {
      // When limit is MAX_RESULT_LIMIT, construct query without LIMIT.
      super(limit == MAX_RESULT_LIMIT ? "" : "LIMIT " + limit,
          "LIMIT " + ClusterIntegrationTestUtils.MAX_NUM_ROWS_TO_COMPARE);
    }
  }

  /**
   * Order by query fragment for aggregation queries.
   * <ul>
   *   <li>SELECT ... FROM ... WHERE ... 'ORDER BY ...'</li>
   * </ul>
   */
  private static class OrderByQueryFragment extends StringQueryFragment {
    OrderByQueryFragment(Set<String> columns) {
      super(columns.isEmpty() ? "" : "ORDER BY " + StringUtils.join(columns, ", "),
          columns.isEmpty() ? "" : "ORDER BY " + StringUtils.join(
              columns.stream().map(c -> String.format("`%s`", c)).collect(Collectors.toList()), ", "));
    }
  }

  /**
   * Predicate query fragment.
   * <ul>
   *   <li>SELECT ... FROM ... 'WHERE ...'</li>
   * </ul>
   */
  private static class PredicateQueryFragment implements QueryFragment {
    final List<QueryFragment> _predicates;
    final List<QueryFragment> _operators;

    /**
     * Constructor for <code>PredicateQueryFragment</code>.
     *
     * @param predicates predicates.
     * @param operators operators between predicates.
     */
    PredicateQueryFragment(List<QueryFragment> predicates, List<QueryFragment> operators) {
      _predicates = predicates;
      _operators = operators;
    }

    @Override
    public String generatePinotQuery() {
      if (_predicates.isEmpty()) {
        return "";
      } else {
        StringBuilder pinotQuery = new StringBuilder("WHERE ");

        // One less than the number of predicates.
        int operatorCount = _operators.size();
        for (int i = 0; i < operatorCount; i++) {
          pinotQuery.append(_predicates.get(i).generatePinotQuery()).append(' ')
              .append(_operators.get(i).generatePinotQuery()).append(' ');
        }
        pinotQuery.append(_predicates.get(operatorCount).generatePinotQuery());

        return pinotQuery.toString();
      }
    }

    @Override
    public String generateH2Query() {
      if (_predicates.isEmpty()) {
        return "";
      } else {
        StringBuilder h2Query = new StringBuilder("WHERE ");

        // One less than the number of predicates.
        int operatorCount = _operators.size();
        for (int i = 0; i < operatorCount; i++) {
          h2Query.append(_predicates.get(i).generateH2Query()).append(' ').append(_operators.get(i).generateH2Query())
              .append(' ');
        }
        h2Query.append(_predicates.get(operatorCount).generateH2Query());

        return h2Query.toString();
      }
    }
  }

  /**
   * Having query fragment.
   * <ul>
   *   <li>SELECT ... FROM ... WHERE ... GROUP BY ... 'HAVING ...'</li>
   * </ul>
   */
  private static class HavingQueryFragment implements QueryFragment {
    final List<String> _havingClauseAggregationFunctions;
    final List<String> _havingClauseOperatorsAndValues;
    final List<String> _havingClauseBooleanOperators;

    HavingQueryFragment(List<String> havingClauseAggregationFunctions, List<String> havingClauseOperatorsAndValues,
        List<String> havingClauseBooleanOperators) {
      _havingClauseAggregationFunctions = havingClauseAggregationFunctions;
      _havingClauseOperatorsAndValues = havingClauseOperatorsAndValues;
      _havingClauseBooleanOperators = havingClauseBooleanOperators;
    }

    @Override
    public String generatePinotQuery() {
      String pinotQuery = "";
      int aggregationFunctionCount = _havingClauseAggregationFunctions.size();
      if (aggregationFunctionCount > 0) {
        pinotQuery += "HAVING";
        pinotQuery = joinWithSpaces(pinotQuery, _havingClauseAggregationFunctions.get(0));
        pinotQuery = joinWithSpaces(pinotQuery, _havingClauseOperatorsAndValues.get(0));
        for (int i = 1; i < aggregationFunctionCount; i++) {
          pinotQuery = joinWithSpaces(pinotQuery, _havingClauseBooleanOperators.get(i - 1));
          pinotQuery = joinWithSpaces(pinotQuery, _havingClauseAggregationFunctions.get(i));
          pinotQuery = joinWithSpaces(pinotQuery, _havingClauseOperatorsAndValues.get(i));
        }
      }
      return pinotQuery;
    }

    @Override
    public String generateH2Query() {
      String h2Query = "";
      int aggregationFunctionCount = _havingClauseAggregationFunctions.size();
      if (aggregationFunctionCount > 0) {
        h2Query += "HAVING";
        String aggregationFunction = _havingClauseAggregationFunctions.get(0);
        if (!aggregationFunction.equals("COUNT(*)")) {
          aggregationFunction = aggregationFunction.replace("(", "(`").replace(")", "`)");
        }
        if (aggregationFunction.startsWith("AVG(")) {
          aggregationFunction = aggregationFunction.replace("AVG(", "AVG(CAST(").replace(")", " AS DOUBLE))");
        } else if (aggregationFunction.startsWith("DISTINCTCOUNT(")) {
          aggregationFunction = aggregationFunction.replace("DISTINCTCOUNT(", "COUNT(DISTINCT ");
        }
        h2Query = joinWithSpaces(h2Query, aggregationFunction);
        h2Query = joinWithSpaces(h2Query, _havingClauseOperatorsAndValues.get(0));
        for (int i = 1; i < aggregationFunctionCount; i++) {
          aggregationFunction = _havingClauseAggregationFunctions.get(i);
          if (!aggregationFunction.equals("COUNT(*)")) {
            aggregationFunction = aggregationFunction.replace("(", "(`").replace(")", "`)");
          }
          if (aggregationFunction.startsWith("AVG(")) {
            aggregationFunction = aggregationFunction.replace("AVG(", "AVG(CAST(").replace(")", " AS DOUBLE))");
          } else if (aggregationFunction.startsWith("DISTINCTCOUNT(")) {
            aggregationFunction = aggregationFunction.replace("DISTINCTCOUNT(", "COUNT(DISTINCT ");
          }
          h2Query = joinWithSpaces(h2Query, _havingClauseBooleanOperators.get(i - 1));
          h2Query = joinWithSpaces(h2Query, aggregationFunction);
          h2Query = joinWithSpaces(h2Query, _havingClauseOperatorsAndValues.get(i));
        }
      }
      return h2Query;
    }
  }

  /**
   * Strategy to generate selection queries.
   * <ul>
   *   <li>SELECT a, b FROM table WHERE a = 'foo' AND b = 'bar' ORDER BY c LIMIT 10</li>
   * </ul>
   */
  private class SelectionQueryGenerationStrategy implements QueryGenerationStrategy {

    @Override
    public Query generateQuery() {
      // Select at most MAX_NUM_SELECTION_COLUMNS columns.
      int projectionColumnCount = Math.min(RANDOM.nextInt(MAX_NUM_SELECTION_COLUMNS) + 1, _columnNames.size());
      if (_useMultistageEngine) {
        projectionColumnCount = Math.min(projectionColumnCount, _singleValueColumnNames.size());
      }
      Set<String> projectionColumns = new HashSet<>();
      while (projectionColumns.size() < projectionColumnCount) {
        projectionColumns.add(pickRandom(_useMultistageEngine ? _singleValueColumnNames : _columnNames));
      }

      // Select at most MAX_NUM_ORDER_BY_COLUMNS columns for ORDER BY clause.
      int orderByColumnCount = Math.min(RANDOM.nextInt(MAX_NUM_ORDER_BY_COLUMNS + 1), _singleValueColumnNames.size());
      Set<String> orderByColumns = new HashSet<>();
      while (orderByColumns.size() < orderByColumnCount) {
        orderByColumns.add(pickRandom(_singleValueColumnNames));
      }

      // Generate a predicate.
      PredicateQueryFragment predicate = generatePredicate();

      // Generate a result limit of at most MAX_RESULT_LIMIT columns for ORDER BY clause.
      int resultLimit = RANDOM.nextInt(MAX_RESULT_LIMIT + 1);
      LimitQueryFragment limit = new LimitQueryFragment(resultLimit);

      return new SelectionQuery(new ArrayList<>(projectionColumns), predicate, new OrderByQueryFragment(orderByColumns),
          limit);
    }
  }

  /**
   * Strategy to generate aggregation queries.
   * <ul>
   *   <li>SELECT SUM(a), MAX(b) FROM table WHERE a = 'foo' AND b = 'bar' GROUP BY c TOP 10</li>
   * </ul>
   */
  private class AggregationQueryGenerationStrategy implements QueryGenerationStrategy {

    @Override
    public Query generateQuery() {
      // Generate at most MAX_NUM_GROUP_BY_COLUMNS columns on which to group.
      int groupColumnCount = Math.min(RANDOM.nextInt(MAX_NUM_GROUP_BY_COLUMNS + 1), _singleValueColumnNames.size());
      Set<String> groupColumns = new HashSet<>();
      while (groupColumns.size() < groupColumnCount) {
        groupColumns.add(pickRandom(_singleValueColumnNames));
      }

      // Generate at most MAX_NUM_AGGREGATION_COLUMNS columns on which to aggregate
      int aggregationColumnCount = RANDOM.nextInt(MAX_NUM_AGGREGATION_COLUMNS + 1);
      Set<String> aggregationColumnsAndFunctions = new HashSet<>();
      boolean isDistinctQuery = false;
      if (aggregationColumnCount == 0) {
        // if no aggregation function being randomly generated, pick the group by columns into the select list
        // this should generate a distinct query using query rewriter.
        // TODO: noted that we don't support distinct/agg rewrite with only part of the group by columns. change this
        // test once we support such queries.
        if (groupColumnCount != 0) {
          aggregationColumnsAndFunctions.addAll(groupColumns);
          isDistinctQuery = true;
        } else {
          aggregationColumnsAndFunctions.add("COUNT(*)");
        }
      } else {
        while (aggregationColumnsAndFunctions.size() < aggregationColumnCount) {
          aggregationColumnsAndFunctions.add(createRandomAggregationFunction());
        }
      }

      // Generate a predicate.
      PredicateQueryFragment predicate = generatePredicate();

      //Generate a HAVING predicate
      ArrayList<String> arrayOfAggregationColumnsAndFunctions = new ArrayList<>(aggregationColumnsAndFunctions);
      HavingQueryFragment havingPredicate;
      if (isDistinctQuery) {
        havingPredicate = new HavingQueryFragment(Collections.emptyList(), Collections.emptyList(),
            Collections.emptyList());
      } else {
        havingPredicate = generateHavingPredicate(arrayOfAggregationColumnsAndFunctions);
      }

      // Generate a result limit of at most MAX_RESULT_LIMIT.
      LimitQueryFragment limit;
      if (isDistinctQuery) {
        // Distinct query must have positive LIMIT
        limit = new LimitQueryFragment(RANDOM.nextInt(MAX_RESULT_LIMIT) + 1);
      } else {
        limit = new LimitQueryFragment(RANDOM.nextInt(MAX_RESULT_LIMIT + 1));
      }

      return new AggregationQuery(arrayOfAggregationColumnsAndFunctions, predicate, groupColumns, havingPredicate,
          limit);
    }

    /**
     * Helper method to generate a having predicate query fragment.
     *
     * @return generated predicate query fragment.
     */
    private HavingQueryFragment generateHavingPredicate(ArrayList<String> arrayOfAggregationColumnsAndFunctions) {
      //Generate a HAVING clause for group by query
      List<String> havingClauseAggregationFunctions = new ArrayList<>();
      List<String> havingClauseOperatorsAndValues = new ArrayList<>();
      List<String> havingClauseBooleanOperators = new ArrayList<>();
      createHavingClause(arrayOfAggregationColumnsAndFunctions, havingClauseAggregationFunctions,
          havingClauseOperatorsAndValues, havingClauseBooleanOperators);
      return new HavingQueryFragment(havingClauseAggregationFunctions, havingClauseOperatorsAndValues,
          havingClauseBooleanOperators);
    }

    private void createHavingClause(ArrayList<String> arrayOfAggregationColumnsAndFunctions,
        List<String> havingClauseAggregationFunctions, List<String> havingClauseOperatorsAndValues,
        List<String> havingClauseBooleanOperators) {
      int numOfFunctionsInSelectList = arrayOfAggregationColumnsAndFunctions.size();
      int aggregationFunctionCount = RANDOM.nextInt(numOfFunctionsInSelectList + 1);
      for (int i = 0; i < aggregationFunctionCount; i++) {
        int aggregationFunctionIndex = RANDOM.nextInt(numOfFunctionsInSelectList);
        String aggregationFunction = arrayOfAggregationColumnsAndFunctions.get(aggregationFunctionIndex);
        havingClauseAggregationFunctions.add(aggregationFunction);
        havingClauseOperatorsAndValues.add(createOperatorAndValueForAggregationFunction(aggregationFunction));
      }
      int aggregationFunctionNotInSelectListCount = RANDOM.nextInt(MAX_NUM_AGGREGATION_COLUMNS + 1);
      for (int i = 0; i < aggregationFunctionNotInSelectListCount; i++) {
        String aggregationFunction = createRandomAggregationFunction();
        havingClauseAggregationFunctions.add(aggregationFunction);
        havingClauseOperatorsAndValues.add(createOperatorAndValueForAggregationFunction(aggregationFunction));
      }
      aggregationFunctionCount += aggregationFunctionNotInSelectListCount;
      for (int i = 1; i < aggregationFunctionCount; i++) {
        havingClauseBooleanOperators.add(pickRandom(BOOLEAN_OPERATORS));
      }
    }

    private String createRandomAggregationFunction() {
      String aggregationFunction = pickRandom(_useMultistageEngine ? MULTI_STAGE_AGGREGATION_FUNCTIONS
          : AGGREGATION_FUNCTIONS);
      String aggregationColumn;
      switch (aggregationFunction) {
        // "COUNT" and "DISTINCTCOUNT" support all single-value columns.
        case "COUNT":
        case "DISTINCTCOUNT":
          aggregationColumn = pickRandom(_singleValueColumnNames);
          break;
        // Other functions only support single-value numeric columns.
        default:
          aggregationColumn = pickRandom(_singleValueNumericalColumnNames);
          break;
      }
      return aggregationFunction + "(" + aggregationColumn + ")";
    }

    private String createOperatorAndValueForAggregationFunction(String aggregationFunction) {
      boolean generateInt = aggregationFunction.startsWith("COUNT") || aggregationFunction.startsWith("DISTINCTCOUNT");
      boolean comparisonOperatorNotBetweenOrIN = RANDOM.nextBoolean();
      if (comparisonOperatorNotBetweenOrIN) {
        return pickRandom(COMPARISON_OPERATORS) + " " + generateRandomValue(generateInt);
      } else {
        boolean isItBetween = RANDOM.nextBoolean();
        if (isItBetween) {
          return String.format("BETWEEN %s AND %s", generateRandomValue(generateInt), generateRandomValue(generateInt));
        } else {
          int numValues = RANDOM.nextInt(MAX_NUM_IN_CLAUSE_VALUES) + 1;
          Set<String> values = new HashSet<>();
          while (values.size() < numValues) {
            values.add(generateRandomValue(generateInt));
          }
          String valuesString = StringUtils.join(values, ", ");
          boolean isItIn = RANDOM.nextBoolean();
          return String.format("%s IN (%s)", isItIn ? "" : "NOT ", valuesString);
        }
      }
    }

    private String generateRandomValue(boolean generateInt) {
      return generateInt ? Integer.toString(RANDOM.nextInt(MAX_INT_FOR_HAVING_CLAUSE_PREDICATE) + 1)
          : Float.toString(RANDOM.nextFloat() * MAX_FLOAT_FOR_HAVING_CLAUSE_PREDICATE);
    }
  }

  /**
   * Generator for single-value column comparison predicate query fragment.
   */
  private class SingleValueComparisonPredicateGenerator implements PredicateGenerator {

    @Override
    public QueryFragment generatePredicate(String columnName, boolean useMultistageEngine) {
      String columnValue = pickRandom(_columnToValueList.get(columnName));
      String comparisonOperator = pickRandom(COMPARISON_OPERATORS);
      return new StringQueryFragment(joinWithSpaces(columnName, comparisonOperator, columnValue),
          joinWithSpaces(String.format("`%s`", columnName), comparisonOperator, columnValue));
    }
  }

  /**
   * Generator for single-value column <code>IN</code> predicate query fragment.
   */
  private class SingleValueInPredicateGenerator implements PredicateGenerator {

    @Override
    public QueryFragment generatePredicate(String columnName, boolean useMultistageEngine) {
      List<String> columnValues = _columnToValueList.get(columnName);

      int numValues = Math.min(RANDOM.nextInt(MAX_NUM_IN_CLAUSE_VALUES) + 1, columnValues.size());
      Set<String> values = new HashSet<>();
      while (values.size() < numValues) {
        values.add(pickRandom(columnValues));
      }
      String inValues = StringUtils.join(values, ", ");

      boolean notIn = RANDOM.nextBoolean();
      if (notIn) {
        return new StringQueryFragment(String.format("%s NOT IN (%s)", columnName, inValues),
            String.format("`%s` NOT IN (%s)", columnName, inValues));
      } else {
        return new StringQueryFragment(String.format("%s IN (%s)", columnName, inValues),
            String.format("`%s` IN (%s)", columnName, inValues));
      }
    }
  }

  /**
   * Generator for single-value column <code>BETWEEN</code> predicate query fragment.
   */
  private class SingleValueBetweenPredicateGenerator implements PredicateGenerator {

    @Override
    public QueryFragment generatePredicate(String columnName, boolean useMultistageEngine) {
      List<String> columnValues = _columnToValueList.get(columnName);
      String leftValue = pickRandom(columnValues);
      String rightValue = pickRandom(columnValues);
      return new StringQueryFragment(
          String.format("%s BETWEEN %s AND %s", columnName, leftValue, rightValue),
          String.format("`%s` BETWEEN %s AND %s", columnName, leftValue, rightValue));
    }
  }

  /**
   * Generator for single-value column <code>REGEX</code> predicate query fragment.
   */
  private class SingleValueRegexPredicateGenerator implements PredicateGenerator {
    Random _random = new Random();

    @Override
    public QueryFragment generatePredicate(String columnName, boolean useMultistageEngine) {
      List<String> columnValues = _columnToValueList.get(columnName);
      String value = pickRandom(columnValues);
      // do regex only for string type
      if (value.startsWith("'") && value.endsWith("'")) {
        // replace only one character for now with .* ignore the first and last character
        int indexToReplaceWithRegex = 1 + _random.nextInt(value.length() - 2);
        String regex = value.substring(1, indexToReplaceWithRegex) + ".*" + value.substring(indexToReplaceWithRegex + 1,
            value.length() - 1);
        String regexpPredicate = String.format(" REGEXP_LIKE(%s, '%s')", columnName, regex);
        String h2RegexpPredicate = String.format(" REGEXP_LIKE(`%s`, '%s', 'i')", columnName, regex);
        return new StringQueryFragment(regexpPredicate, h2RegexpPredicate);
      } else {
        String equalsPredicate = String.format("%s = %s", columnName, value);
        String h2EqualsPredicate = String.format("`%s` = %s", columnName, value);
        return new StringQueryFragment(equalsPredicate, h2EqualsPredicate);
      }
    }
  }

  /**
   * Generator for multi-value column comparison predicate query fragment.
   * <p>DO NOT SUPPORT '<code>NOT EQUAL</code>'.
   */
  private class MultiValueComparisonPredicateGenerator implements PredicateGenerator {

    @Override
    public QueryFragment generatePredicate(String columnName, boolean useMultistageEngine) {
      String columnValue = pickRandom(_columnToValueList.get(columnName));
      String comparisonOperator = pickRandom(COMPARISON_OPERATORS);

      // Not equal works differently on multi-value, so avoid '<>' comparison.
      while (comparisonOperator.equals("<>")) {
        comparisonOperator = pickRandom(COMPARISON_OPERATORS);
      }

      List<String> h2ComparisonClauses =
          new ArrayList<>(ClusterIntegrationTestUtils.MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE);
      for (int i = 1; i <= ClusterIntegrationTestUtils.MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE; i++) {
        h2ComparisonClauses.add(
            joinWithSpaces(String.format("%s[%d]", columnName, i), comparisonOperator, columnValue));
      }

      return new StringQueryFragment(
          joinWithSpaces(generateMultiValueColumn(columnName, useMultistageEngine), comparisonOperator, columnValue),
          generateH2QueryConditionPredicate(h2ComparisonClauses));
    }
  }

  /**
   * Generator for multi-value column <code>IN</code> predicate query fragment.
   * <p>DO NOT SUPPORT '<code>NOT IN</code>'.
   */
  private class MultiValueInPredicateGenerator implements PredicateGenerator {

    @Override
    public QueryFragment generatePredicate(String columnName, boolean useMultistageEngine) {
      List<String> columnValues = _columnToValueList.get(columnName);

      int numValues = Math.min(RANDOM.nextInt(MAX_NUM_IN_CLAUSE_VALUES) + 1, columnValues.size());
      Set<String> values = new HashSet<>();
      while (values.size() < numValues) {
        values.add(pickRandom(columnValues));
      }
      String inValues = StringUtils.join(values, ", ");

      List<String> h2InClauses =
          new ArrayList<>(ClusterIntegrationTestUtils.MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE);
      for (int i = 1; i <= ClusterIntegrationTestUtils.MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE; i++) {
        h2InClauses.add(String.format("%s[%d] IN (%s)", columnName, i, inValues));
      }

      return new StringQueryFragment(
          String.format("%s IN (%s)", generateMultiValueColumn(columnName, useMultistageEngine), inValues),
          generateH2QueryConditionPredicate(h2InClauses));
    }
  }

  /**
   * Generator for multi-value column <code>BETWEEN</code> predicate query fragment.
   */
  private class MultiValueBetweenPredicateGenerator implements PredicateGenerator {

    @Override
    public QueryFragment generatePredicate(String columnName, boolean useMultistageEngine) {
      List<String> columnValues = _columnToValueList.get(columnName);
      String leftValue = pickRandom(columnValues);
      String rightValue = pickRandom(columnValues);

      List<String> h2ComparisonClauses =
          new ArrayList<>(ClusterIntegrationTestUtils.MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE);
      for (int i = 1; i <= ClusterIntegrationTestUtils.MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE; i++) {
        h2ComparisonClauses.add(String.format("%s[%d] BETWEEN %s AND %s", columnName, i, leftValue, rightValue));
      }

      return new StringQueryFragment(
          String.format("%s BETWEEN %s AND %s", generateMultiValueColumn(columnName, useMultistageEngine), leftValue,
              rightValue),
          generateH2QueryConditionPredicate(h2ComparisonClauses));
    }
  }

  private String generateMultiValueColumn(String columnName, boolean useMultistageEngine) {
    if (useMultistageEngine) {
      return String.format("ARRAY_TO_MV(%s)", columnName);
    }
    return columnName;
  }

  private static String generateH2QueryConditionPredicate(List<String> conditionList) {
    return generateH2QueryConditionPredicate(conditionList, " OR ");
  }

  private static String generateH2QueryConditionPredicate(List<String> conditionList, String separator) {
    return String.format("( %s )", StringUtils.join(conditionList, separator));
  }
}
