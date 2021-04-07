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
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * The <code>QueryGenerator</code> class is used to generate random equivalent PQL/SQL query pairs based on Avro files.
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
  private static final int MAX_COUNT_FUNCTION_RESULT = 1000;
  private static final float MAX_FLOAT_FOR_HAVING_CLAUSE_PREDICATE = 10000;
  private static final int MAX_INT_FOR_HAVING_CLAUSE_PREDICATE = 10000;
  private static final List<String> BOOLEAN_OPERATORS = Arrays.asList("OR", "AND");
  private static final List<String> COMPARISON_OPERATORS = Arrays.asList("=", "<>", "<", ">", "<=", ">=");

  // TODO: fix DISTINCTCOUNT implementation and add it back.
  // Currently for DISTINCTCOUNT we use hashcode as the key, should change it to use raw values.
  private static final List<String> AGGREGATION_FUNCTIONS =
      Arrays.asList("SUM", "MIN", "MAX", "AVG", "COUNT"/*, "DISTINCTCOUNT"*/);
  private static final Random RANDOM = new Random();

  private final Map<String, Set<String>> _columnToValueSet = new HashMap<>();
  private final Map<String, List<String>> _columnToValueList = new HashMap<>();
  private final List<String> _columnNames = new ArrayList<>();
  private final List<String> _singleValueColumnNames = new ArrayList<>();
  private final List<String> _singleValueNumericalColumnNames = new ArrayList<>();
  private final Map<String, Integer> _multiValueColumnMaxNumElements = new HashMap<>();

  private final List<QueryGenerationStrategy> _queryGenerationStrategies =
      Arrays.asList(new SelectionQueryGenerationStrategy(), new AggregationQueryGenerationStrategy());
  private final List<PredicateGenerator> _singleValuePredicateGenerators = Arrays
      .asList(new SingleValueComparisonPredicateGenerator(), new SingleValueInPredicateGenerator(),
          new SingleValueBetweenPredicateGenerator(), new SingleValueRegexPredicateGenerator());
  private final List<PredicateGenerator> _multiValuePredicateGenerators = Arrays
      .asList(new MultiValueComparisonPredicateGenerator(), new MultiValueInPredicateGenerator(),
          new MultiValueBetweenPredicateGenerator());

  private final String _pinotTableName;
  private final String _h2TableName;
  private boolean _skipMultiValuePredicates = false;

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
            _columnToValueSet.put(fieldName, new HashSet<String>());
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
            _columnToValueSet.put(fieldName, new HashSet<String>());
            _multiValueColumnMaxNumElements.put(fieldName, 0);
            break;
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
            _columnNames.add(fieldName);
            _columnToValueSet.put(fieldName, new HashSet<String>());
            _singleValueColumnNames.add(fieldName);
            _singleValueNumericalColumnNames.add(fieldName);
            break;
          case BOOLEAN:
          case STRING:
            _columnNames.add(fieldName);
            _columnToValueSet.put(fieldName, new HashSet<String>());
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
        "pinot-integration-tests/src/test/resources/On_Time_On_Time_Performance_2014_100k_subset.test_queries_10K.pql");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
      for (int i = 0; i < 10000; i++) {
        Query query = queryGenerator.generateQuery();
        ObjectNode queryJson = JsonUtils.newObjectNode();
        queryJson.put("pql", query.generatePql());
        queryJson.set("hsqls", JsonUtils.objectToJsonNode(query.generateH2Sql()));
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
          predicates.add(pickRandom(_singleValuePredicateGenerators).generatePredicate(columnName));
        } else if (!_skipMultiValuePredicates) {
          // Multi-value column.
          predicates.add(pickRandom(_multiValuePredicateGenerators).generatePredicate(columnName));
        }
      }
    }

    if (predicateCount < 2) {
      // No need to join.
      return new PredicateQueryFragment(predicates, Collections.<QueryFragment>emptyList());
    } else {
      // Join predicates with ANDs and ORs.
      List<QueryFragment> operators = new ArrayList<>(predicateCount - 1);
      for (int i = 1; i < predicateCount; i++) {
        operators.add(new StringQueryFragment(pickRandom(BOOLEAN_OPERATORS)));
      }
      return new PredicateQueryFragment(predicates, operators);
    }
  }

  /**
   * Query interface with capability of generating PQL and H2 SQL query.
   */
  public interface Query {

    /**
     * Generate PQL query.
     *
     * @return generated PQL query.
     */
    String generatePql();

    /**
     * Generate H2 SQL queries equivalent to the PQL query.
     *
     * @return generated H2 SQL queries.
     */
    List<String> generateH2Sql();
  }

  /**
   * Query fragment interface with capability of generating PQL and H2 SQL query fragment.
   */
  private interface QueryFragment {

    /**
     * Generate PQL query fragment.
     *
     * @return generated PQL query fragment.
     */
    String generatePql();

    /**
     * Generate H2 SQL query fragment equivalent to the PQL query fragment.
     *
     * @return generated H2 SQL query fragment.
     */
    String generateH2Sql();
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
     * @param columnName column name.
     * @return generated predicate query fragment.
     */
    QueryFragment generatePredicate(String columnName);
  }

  /**
   * Selection query.
   */
  private class SelectionQuery implements Query {
    private final List<String> _projectionColumns;
    private final PredicateQueryFragment _predicate;
    private final OrderByQueryFragment _orderBy;
    private final LimitQueryFragment _limit;

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
    public String generatePql() {
      return joinWithSpaces("SELECT", StringUtils.join(_projectionColumns, ", "), "FROM", _pinotTableName,
          _predicate.generatePql(), _orderBy.generatePql(), _limit.generatePql());
    }

    @Override
    public List<String> generateH2Sql() {
      List<String> h2ProjectionColumns = new ArrayList<>();
      for (String projectionColumn : _projectionColumns) {
        if (_multiValueColumnMaxNumElements.containsKey(projectionColumn)) {
          // Multi-value column.
          for (int i = 0; i < ClusterIntegrationTestUtils.MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE; i++) {
            h2ProjectionColumns.add(projectionColumn + "__MV" + i);
          }
        } else {
          // Single-value column.
          h2ProjectionColumns.add(projectionColumn);
        }
      }
      return Collections.singletonList(
          joinWithSpaces("SELECT", StringUtils.join(h2ProjectionColumns, ", "), "FROM", _h2TableName,
              _predicate.generateH2Sql(), _orderBy.generateH2Sql(), _limit.generateH2Sql()));
    }
  }

  /**
   * Aggregation query.
   */
  private class AggregationQuery implements Query {
    private List<String> _aggregateColumnsAndFunctions;
    private PredicateQueryFragment _predicate;
    private HavingQueryFragment _havingPredicate;
    private Set<String> _groupColumns;
    private TopQueryFragment _top;

    /**
     * Constructor for <code>AggregationQuery</code>.
     *
     * @param aggregateColumnsAndFunctions aggregation functions.
     * @param predicate predicate fragment.
     * @param groupColumns group-by columns.
     * @param top top fragment.
     */
    public AggregationQuery(List<String> aggregateColumnsAndFunctions, PredicateQueryFragment predicate,
        Set<String> groupColumns, TopQueryFragment top, HavingQueryFragment havingPredicate) {
      _aggregateColumnsAndFunctions = aggregateColumnsAndFunctions;
      _predicate = predicate;
      _groupColumns = groupColumns;
      _top = top;
      _havingPredicate = havingPredicate;
    }

    @Override
    public String generatePql() {
      if (_groupColumns.isEmpty()) {
        return joinWithSpaces("SELECT", StringUtils.join(_aggregateColumnsAndFunctions, ", "), "FROM", _pinotTableName,
            _predicate.generatePql(), _top.generatePql());
      } else {
        String groupByColumns = StringUtils.join(_groupColumns, ", ");

        // TODO: After fixing having clause, we need to add back the having predicate.
        return joinWithSpaces("SELECT", StringUtils.join(_aggregateColumnsAndFunctions, ", "), "FROM", _pinotTableName,
            _predicate.generatePql(), "GROUP BY", StringUtils.join(_groupColumns, ", "), _top.generatePql());
      }
    }

    @Override
    public List<String> generateH2Sql() {
      List<String> queries = new ArrayList<>();

      // For each aggregation function, generate one separate H2 SQL query.
      for (String aggregateColumnAndFunction : _aggregateColumnsAndFunctions) {
        // Make 'AVG' and 'DISTINCTCOUNT' compatible with H2 SQL query.
        if (aggregateColumnAndFunction.startsWith("AVG(")) {
          aggregateColumnAndFunction =
              aggregateColumnAndFunction.replace("AVG(", "AVG(CAST(").replace(")", " AS DOUBLE))");
        } else if (aggregateColumnAndFunction.startsWith("DISTINCTCOUNT(")) {
          aggregateColumnAndFunction = aggregateColumnAndFunction.replace("DISTINCTCOUNT(", "COUNT(DISTINCT ");
        }

        if (_groupColumns.isEmpty()) {
          // Aggregation query.
          queries.add(
              joinWithSpaces("SELECT", aggregateColumnAndFunction, "FROM", _h2TableName, _predicate.generateH2Sql(),
                  _top.generateH2Sql()));
        } else {
          // Group-by query.

          // Unlike PQL, SQL expects the group columns in select statements.
          String groupByColumns = StringUtils.join(_groupColumns, ", ");

          // TODO: After fixing having clause, we need to add back the having predicate.
          queries.add(joinWithSpaces("SELECT", groupByColumns + ",", aggregateColumnAndFunction, "FROM", _h2TableName,
              _predicate.generateH2Sql(), "GROUP BY", groupByColumns, _top.generateH2Sql()));
        }
      }

      return queries;
    }
  }

  /**
   * Most basic query fragment.
   */
  private class StringQueryFragment implements QueryFragment {
    String _pql;
    String _sql;

    /**
     * Constructor for with same PQL and H2 SQL query fragment.
     *
     * @param pql PQL (H2 SQL) query fragment.
     */
    StringQueryFragment(String pql) {
      _pql = pql;
      _sql = pql;
    }

    /**
     * Constructor for <code>StringQueryFragment</code> with different PQL and H2 SQL query fragment.
     *
     * @param pql PQL query fragment.
     * @param sql H2 SQL query fragment.
     */
    StringQueryFragment(String pql, String sql) {
      _pql = pql;
      _sql = sql;
    }

    @Override
    public String generatePql() {
      return _pql;
    }

    @Override
    public String generateH2Sql() {
      return _sql;
    }
  }

  /**
   * Limit query fragment for selection queries.
   * <ul>
   *   <li>SELECT ... FROM ... WHERE ... 'LIMIT ...'</li>
   * </ul>
   */
  private class LimitQueryFragment extends StringQueryFragment {
    LimitQueryFragment(int limit) {
      // When limit is MAX_RESULT_LIMIT, construct query without LIMIT.
      super(limit == MAX_RESULT_LIMIT ? "" : "LIMIT " + limit,
          "LIMIT " + ClusterIntegrationTestUtils.MAX_NUM_ROWS_TO_COMPARE);
    }
  }

  /**
   * Top query fragment for aggregation queries.
   * <ul>
   *   <li>SELECT ... FROM ... WHERE ... 'TOP ...'</li>
   * </ul>
   */
  private class TopQueryFragment extends StringQueryFragment {
    TopQueryFragment(int top) {
      // When top is MAX_RESULT_LIMIT, construct query without TOP.
      super(top == MAX_RESULT_LIMIT ? "" : "TOP " + top,
          "LIMIT " + ClusterIntegrationTestUtils.MAX_NUM_ROWS_TO_COMPARE);
    }
  }

  /**
   * Order by query fragment for aggregation queries.
   * <ul>
   *   <li>SELECT ... FROM ... WHERE ... 'ORDER BY ...'</li>
   * </ul>
   */
  private class OrderByQueryFragment extends StringQueryFragment {
    OrderByQueryFragment(Set<String> columns) {
      super(columns.isEmpty() ? "" : "ORDER BY " + StringUtils.join(columns, ", "));
    }
  }

  /**
   * Predicate query fragment.
   * <ul>
   *   <li>SELECT ... FROM ... 'WHERE ...'</li>
   * </ul>
   */
  private class PredicateQueryFragment implements QueryFragment {
    List<QueryFragment> _predicates;
    List<QueryFragment> _operators;

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
    public String generatePql() {
      if (_predicates.isEmpty()) {
        return "";
      } else {
        StringBuilder pql = new StringBuilder("WHERE ");

        // One less than the number of predicates.
        int operatorCount = _operators.size();
        for (int i = 0; i < operatorCount; i++) {
          pql.append(_predicates.get(i).generatePql()).append(' ').append(_operators.get(i).generatePql()).append(' ');
        }
        pql.append(_predicates.get(operatorCount).generatePql());

        return pql.toString();
      }
    }

    @Override
    public String generateH2Sql() {
      if (_predicates.isEmpty()) {
        return "";
      } else {
        StringBuilder sql = new StringBuilder("WHERE ");

        // One less than the number of predicates.
        int operatorCount = _operators.size();
        for (int i = 0; i < operatorCount; i++) {
          sql.append(_predicates.get(i).generateH2Sql()).append(' ').append(_operators.get(i).generateH2Sql())
              .append(' ');
        }
        sql.append(_predicates.get(operatorCount).generateH2Sql());

        return sql.toString();
      }
    }
  }

  /**
   * Predicate query fragment.
   * <ul>
   *   <li>SELECT ... FROM ... 'WHERE ... GROUP BY ... HAVING ...'</li>
   * </ul>
   */
  private class HavingQueryFragment implements QueryFragment {
    private List<String> _havingClauseAggregationFunctions;
    private List<String> _havingClauseOperatorsAndValues;
    private List<String> _havingClauseBooleanOperators;

    /**
     * Constructor for <code>PredicateQueryFragment</code>.
     *
     * @param
     * @param
     */
    HavingQueryFragment(List<String> havingClauseAggregationFunctions, List<String> havingClauseOperatorsAndValues,
        List<String> havingClauseBooleanOperators) {
      _havingClauseAggregationFunctions = havingClauseAggregationFunctions;
      _havingClauseOperatorsAndValues = havingClauseOperatorsAndValues;
      _havingClauseBooleanOperators = havingClauseBooleanOperators;
    }

    @Override
    public String generatePql() {
      String pqlHavingClause = "";
      int aggregationFunctionCount = _havingClauseAggregationFunctions.size();
      if (aggregationFunctionCount > 0) {
        pqlHavingClause += "HAVING";
        pqlHavingClause = joinWithSpaces(pqlHavingClause, _havingClauseAggregationFunctions.get(0));
        pqlHavingClause = joinWithSpaces(pqlHavingClause, _havingClauseOperatorsAndValues.get(0));
        for (int i = 1; i < aggregationFunctionCount; i++) {
          pqlHavingClause = joinWithSpaces(pqlHavingClause, _havingClauseBooleanOperators.get(i - 1));
          pqlHavingClause = joinWithSpaces(pqlHavingClause, _havingClauseAggregationFunctions.get(i));
          pqlHavingClause = joinWithSpaces(pqlHavingClause, _havingClauseOperatorsAndValues.get(i));
        }
      }
      return pqlHavingClause;
    }

    @Override
    public String generateH2Sql() {
      String sqlHavingClause = "";
      int aggregationFunctionCount = _havingClauseAggregationFunctions.size();
      if (aggregationFunctionCount > 0) {
        sqlHavingClause += "HAVING";
        String aggregationFunction = _havingClauseAggregationFunctions.get(0);
        if (aggregationFunction.startsWith("AVG(")) {
          aggregationFunction = aggregationFunction.replace("AVG(", "AVG(CAST(").replace(")", " AS DOUBLE))");
        } else if (aggregationFunction.startsWith("DISTINCTCOUNT(")) {
          aggregationFunction = aggregationFunction.replace("DISTINCTCOUNT(", "COUNT(DISTINCT ");
        }
        sqlHavingClause = joinWithSpaces(sqlHavingClause, aggregationFunction);
        sqlHavingClause = joinWithSpaces(sqlHavingClause, _havingClauseOperatorsAndValues.get(0));
        for (int i = 1; i < aggregationFunctionCount; i++) {
          aggregationFunction = _havingClauseAggregationFunctions.get(i);
          if (aggregationFunction.startsWith("AVG(")) {
            aggregationFunction = aggregationFunction.replace("AVG(", "AVG(CAST(").replace(")", " AS DOUBLE))");
          } else if (aggregationFunction.startsWith("DISTINCTCOUNT(")) {
            aggregationFunction = aggregationFunction.replace("DISTINCTCOUNT(", "COUNT(DISTINCT ");
          }
          sqlHavingClause = joinWithSpaces(sqlHavingClause, _havingClauseBooleanOperators.get(i - 1));
          sqlHavingClause = joinWithSpaces(sqlHavingClause, aggregationFunction);
          sqlHavingClause = joinWithSpaces(sqlHavingClause, _havingClauseOperatorsAndValues.get(i));
        }
      }
      return sqlHavingClause;
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
      Set<String> projectionColumns = new HashSet<>();
      while (projectionColumns.size() < projectionColumnCount) {
        projectionColumns.add(pickRandom(_columnNames));
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
      // Generate at most MAX_NUM_AGGREGATION_COLUMNS columns on which to aggregate, map 0 to 'COUNT(*)'.
      int aggregationColumnCount = RANDOM.nextInt(MAX_NUM_AGGREGATION_COLUMNS + 1);
      Set<String> aggregationColumnsAndFunctions = new HashSet<>();
      if (aggregationColumnCount == 0) {
        aggregationColumnsAndFunctions.add("COUNT(*)");
      } else {
        while (aggregationColumnsAndFunctions.size() < aggregationColumnCount) {
          aggregationColumnsAndFunctions.add(createRandomAggregationFunction());
        }
      }
      // Generate a predicate.
      PredicateQueryFragment predicate = generatePredicate();
      // Generate at most MAX_NUM_GROUP_BY_COLUMNS columns on which to group.
      int groupColumnCount = Math.min(RANDOM.nextInt(MAX_NUM_GROUP_BY_COLUMNS + 1), _singleValueColumnNames.size());
      Set<String> groupColumns = new HashSet<>();
      while (groupColumns.size() < groupColumnCount) {
        groupColumns.add(pickRandom(_singleValueColumnNames));
      }
      //Generate a HAVING predicate
      ArrayList<String> arrayOfAggregationColumnsAndFunctions = new ArrayList<>(aggregationColumnsAndFunctions);
      HavingQueryFragment havingPredicate = generateHavingPredicate(arrayOfAggregationColumnsAndFunctions);
      // Generate a result limit of at most MAX_RESULT_LIMIT.
      TopQueryFragment top = new TopQueryFragment(RANDOM.nextInt(MAX_RESULT_LIMIT + 1));
      return new AggregationQuery(arrayOfAggregationColumnsAndFunctions, predicate, groupColumns, top, havingPredicate);
    }

    /**
     * Helper method to generate a having predicate query fragment.
     *
     * @return generated predicate query fragment.
     */
    private HavingQueryFragment generateHavingPredicate(ArrayList<String> arrayOfAggregationColumnsAndFunctions) {
      //Generate a HAVING clause for group by query
      List<String> havingClauseAggregationFunctions = new ArrayList<String>();
      List<String> havingClauseOperatorsAndValues = new ArrayList<String>();
      List<String> havingClauseBooleanOperators = new ArrayList<String>();
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
      ArrayList<String> aggregationPredicates = new ArrayList<>();
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
      String aggregationFunction = pickRandom(AGGREGATION_FUNCTIONS);
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
      }
      return aggregationFunction + "(" + aggregationColumn + ")";
    }

    private String createOperatorAndValueForAggregationFunction(String aggregationFunction) {
      boolean comparisonOperatorNotBetweenOrIN = RANDOM.nextBoolean();
      String valueOperator = new String();
      String comparisonOperator = new String();
      if (comparisonOperatorNotBetweenOrIN) {
        String functionValue = new String();
        comparisonOperator = pickRandom(COMPARISON_OPERATORS);
        if (aggregationFunction.startsWith("COUNT") || aggregationFunction.startsWith("DISTINCTCOUNT")) {
          functionValue = Integer.toString(RANDOM.nextInt(MAX_COUNT_FUNCTION_RESULT) + 1);
        } else {
          functionValue = Float.toString(RANDOM.nextFloat() * MAX_FLOAT_FOR_HAVING_CLAUSE_PREDICATE);
        }
        valueOperator = joinWithSpaces(valueOperator, comparisonOperator);
        valueOperator = joinWithSpaces(valueOperator, functionValue);
      } else {
        boolean isItBetween = RANDOM.nextBoolean();
        if (isItBetween) {
          String leftValue = Float.toString(RANDOM.nextFloat() * MAX_FLOAT_FOR_HAVING_CLAUSE_PREDICATE);
          String rightValue = Float.toString(RANDOM.nextFloat() * MAX_FLOAT_FOR_HAVING_CLAUSE_PREDICATE);
          comparisonOperator = "BETWEEN " + leftValue + " AND " + rightValue;
          valueOperator = comparisonOperator;
        } else {
          int numValues = RANDOM.nextInt(MAX_NUM_IN_CLAUSE_VALUES) + 1;
          Set<String> values = new HashSet<>();
          while (values.size() < numValues) {
            values.add(Integer.toString(RANDOM.nextInt(MAX_INT_FOR_HAVING_CLAUSE_PREDICATE)));
          }
          comparisonOperator = StringUtils.join(values, ", ");
          boolean isItIn = RANDOM.nextBoolean();
          if (isItIn) {
            comparisonOperator = "IN (" + comparisonOperator;
            comparisonOperator += ")";
          } else {
            comparisonOperator = "NOT IN (" + comparisonOperator;
            comparisonOperator += ")";
          }
          valueOperator = comparisonOperator;
        }
      }
      return valueOperator;
    }
  }

  /**
   * Generator for single-value column comparison predicate query fragment.
   */
  private class SingleValueComparisonPredicateGenerator implements PredicateGenerator {

    @Override
    public QueryFragment generatePredicate(String columnName) {
      String columnValue = pickRandom(_columnToValueList.get(columnName));
      String comparisonOperator = pickRandom(COMPARISON_OPERATORS);
      return new StringQueryFragment(joinWithSpaces(columnName, comparisonOperator, columnValue));
    }
  }

  /**
   * Generator for single-value column <code>IN</code> predicate query fragment.
   */
  private class SingleValueInPredicateGenerator implements PredicateGenerator {

    @Override
    public QueryFragment generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);

      int numValues = Math.min(RANDOM.nextInt(MAX_NUM_IN_CLAUSE_VALUES) + 1, columnValues.size());
      Set<String> values = new HashSet<>();
      while (values.size() < numValues) {
        values.add(pickRandom(columnValues));
      }
      String inValues = StringUtils.join(values, ", ");

      boolean notIn = RANDOM.nextBoolean();
      if (notIn) {
        return new StringQueryFragment(columnName + " NOT IN (" + inValues + ")");
      } else {
        return new StringQueryFragment(columnName + " IN (" + inValues + ")");
      }
    }
  }

  /**
   * Generator for single-value column <code>BETWEEN</code> predicate query fragment.
   */
  private class SingleValueBetweenPredicateGenerator implements PredicateGenerator {

    @Override
    public QueryFragment generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);
      String leftValue = pickRandom(columnValues);
      String rightValue = pickRandom(columnValues);
      return new StringQueryFragment(columnName + " BETWEEN " + leftValue + " AND " + rightValue);
    }
  }

  /**
   * Generator for single-value column <code>REGEX</code> predicate query fragment.
   */
  private class SingleValueRegexPredicateGenerator implements PredicateGenerator {
    Random random = new Random();

    @Override
    public QueryFragment generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);
      String value;
      value = pickRandom(columnValues);
      StringBuilder pqlRegexBuilder = new StringBuilder();
      StringBuilder sqlRegexBuilder = new StringBuilder();
      // do regex only for string type
      if (value.startsWith("'") && value.endsWith("'")) {
        // replace only one character for now with .* ignore the first and last character
        int indexToReplaceWithRegex = 1 + random.nextInt(value.length() - 2);
        for (int i = 1; i < value.length() - 1; i++) {
          if (i == indexToReplaceWithRegex) {
            pqlRegexBuilder.append(".*");
            sqlRegexBuilder.append(".*");
          } else {
            pqlRegexBuilder.append(value.charAt(i));
            sqlRegexBuilder.append(value.charAt(i));
          }
        }

        String pql = String.format(" REGEXP_LIKE(%s, '%s')", columnName, pqlRegexBuilder.toString());
        String sql = String.format(" REGEXP_LIKE(%s, '%s', 'i')", columnName, sqlRegexBuilder.toString());
        return new StringQueryFragment(pql, sql);
      } else {
        String equalsPredicate = String.format("%s = %s", columnName, value);
        return new StringQueryFragment(equalsPredicate);
      }
    }
  }

  /**
   * Generator for multi-value column comparison predicate query fragment.
   * <p>DO NOT SUPPORT '<code>NOT EQUAL</code>'.
   */
  private class MultiValueComparisonPredicateGenerator implements PredicateGenerator {

    @Override
    public QueryFragment generatePredicate(String columnName) {
      String columnValue = pickRandom(_columnToValueList.get(columnName));
      String comparisonOperator = pickRandom(COMPARISON_OPERATORS);

      // Not equal works differently on multi-value, so avoid '<>' comparison.
      while (comparisonOperator.equals("<>")) {
        comparisonOperator = pickRandom(COMPARISON_OPERATORS);
      }

      List<String> h2ComparisonClauses =
          new ArrayList<>(ClusterIntegrationTestUtils.MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE);
      for (int i = 0; i < ClusterIntegrationTestUtils.MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE; i++) {
        h2ComparisonClauses.add(joinWithSpaces(columnName + "__MV" + i, comparisonOperator, columnValue));
      }

      return new StringQueryFragment(joinWithSpaces(columnName, comparisonOperator, columnValue),
          "(" + StringUtils.join(h2ComparisonClauses, " OR ") + ")");
    }
  }

  /**
   * Generator for multi-value column <code>IN</code> predicate query fragment.
   * <p>DO NOT SUPPORT '<code>NOT IN</code>'.
   */
  private class MultiValueInPredicateGenerator implements PredicateGenerator {

    @Override
    public QueryFragment generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);

      int numValues = Math.min(RANDOM.nextInt(MAX_NUM_IN_CLAUSE_VALUES) + 1, columnValues.size());
      Set<String> values = new HashSet<>();
      while (values.size() < numValues) {
        values.add(pickRandom(columnValues));
      }
      String inValues = StringUtils.join(values, ", ");

      List<String> h2InClauses =
          new ArrayList<>(ClusterIntegrationTestUtils.MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE);
      for (int i = 0; i < ClusterIntegrationTestUtils.MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE; i++) {
        h2InClauses.add(columnName + "__MV" + i + " IN (" + inValues + ")");
      }

      return new StringQueryFragment(columnName + " IN (" + inValues + ")",
          "(" + StringUtils.join(h2InClauses, " OR ") + ")");
    }
  }

  /**
   * Generator for multi-value column <code>BETWEEN</code> predicate query fragment.
   */
  private class MultiValueBetweenPredicateGenerator implements PredicateGenerator {

    @Override
    public QueryFragment generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);
      String leftValue = pickRandom(columnValues);
      String rightValue = pickRandom(columnValues);

      List<String> h2ComparisonClauses =
          new ArrayList<>(ClusterIntegrationTestUtils.MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE);
      for (int i = 0; i < ClusterIntegrationTestUtils.MAX_NUM_ELEMENTS_IN_MULTI_VALUE_TO_COMPARE; i++) {
        h2ComparisonClauses.add(columnName + "__MV" + i + " BETWEEN " + leftValue + " AND " + rightValue);
      }

      return new StringQueryFragment(columnName + " BETWEEN " + leftValue + " AND " + rightValue,
          "(" + StringUtils.join(h2ComparisonClauses, " OR ") + ")");
    }
  }
}
