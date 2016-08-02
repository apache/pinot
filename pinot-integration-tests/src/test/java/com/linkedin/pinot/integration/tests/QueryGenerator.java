/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.integration.tests;

import java.io.File;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class to generate random SQL queries based on an Avro file.
 *
 * Supports COMPARISON, IN and BETWEEN predicate for both single-value and multi-value columns.
 * - For multi-value columns, does not support NOT EQUAL and NOT IN.
 * Supports single-value data type: BOOLEAN, INT, LONG, FLOAT, DOUBLE, STRING.
 * Supports multi-value data type: INT, LONG, FLOAT, DOUBLE, STRING.
 * Supports aggregation function: SUM, MIN, MAX, AVG, COUNT, DISTINCTCOUNT.
 * - SUM, MIN, MAX, AVG can only work on numeric single-value columns.
 * - COUNT, DISTINCTCOUNT can work on any single-value columns.
 */
public class QueryGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryGenerator.class);

  // Configurable variables.
  private static final int MAX_PREDICATE_COUNT = 3;
  private static final int MAX_IN_CLAUSE_NUM_VALUES = 5;
  private static final int MAX_RESULT_LIMIT = 30;
  private static final int MAX_SELECTION_COLUMNS = 3;
  private static final int MAX_ORDER_BY_COLUMNS = 3;
  private static final int MAX_AGGREGATE_COLUMNS = 3;
  private static final int MAX_GROUP_COLUMNS = 3;

  private final Map<String, Set<String>> _columnToValues = new HashMap<>();
  private final Map<String, List<String>> _columnToValueList = new HashMap<>();
  private final List<String> _columnNames = new ArrayList<>();
  private final List<String> _singleValueColumnNames = new ArrayList<>();
  private final List<String> _singleValueNumericalColumnNames = new ArrayList<>();
  private final Map<String, Integer> _multiValueColumnMaxNumElements = new HashMap<>();

  private static final List<String> BOOLEAN_OPERATORS = Arrays.asList("OR", "AND");
  private static final List<String> COMPARISON_OPERATORS = Arrays.asList("=", "<>", "<", ">", "<=", ">=");
  // TODO: fix DISTINCTCOUNT implementation and add it back.
  // Currently for DISTINCTCOUNT we use hashcode as the key, should change it to use raw values.
  private static final List<String> AGGREGATION_FUNCTIONS =
      Arrays.asList("SUM", "MIN", "MAX", "AVG", "COUNT"/*, "DISTINCTCOUNT"*/);
  private static final Random RANDOM = new Random();

  private final List<QueryGenerationStrategy> _queryGenerationStrategies =
      Arrays.asList(new SelectionQueryGenerationStrategy(), new AggregationQueryGenerationStrategy());
  private final List<PredicateGenerator> _singleValuePredicateGenerators =
      Arrays.asList(new SingleValueComparisonPredicateGenerator(), new SingleValueInPredicateGenerator(),
          new SingleValueBetweenPredicateGenerator());
  private final List<PredicateGenerator> _multiValuePredicateGenerators =
      Arrays.asList(new MultiValueComparisonPredicateGenerator(), new MultiValueInPredicateGenerator(),
          new MultiValueBetweenPredicateGenerator());

  private final String _pqlTableName;
  private final String _h2TableName;
  private boolean _skipMultiValuePredicates = false;

  /**
   * Constructor for query generator.
   *
   * @param avroFiles avro files list.
   * @param pqlTableName Pinot table name.
   * @param h2TableName H2 table name.
   */
  public QueryGenerator(List<File> avroFiles, String pqlTableName, String h2TableName) {
    _pqlTableName = pqlTableName;
    _h2TableName = h2TableName;

    // Read avro schema and initialize storage.
    File schemaAvroFile = avroFiles.get(0);
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

    try (DataFileReader<GenericRecord> fileReader = new DataFileReader<>(schemaAvroFile, datumReader)) {
      Schema schema = fileReader.getSchema();
      for (Schema.Field field : schema.getFields()) {
        Schema fieldSchema = field.schema();
        Schema.Type fieldType = fieldSchema.getType();
        String fieldName = field.name();

        switch (fieldType) {
          case UNION:
            _columnNames.add(fieldName);
            _columnToValues.put(fieldName, new HashSet<String>());
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
            _columnToValues.put(fieldName, new HashSet<String>());
            _multiValueColumnMaxNumElements.put(fieldName, 0);
            break;
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
            _columnNames.add(fieldName);
            _columnToValues.put(fieldName, new HashSet<String>());
            _singleValueColumnNames.add(fieldName);
            _singleValueNumericalColumnNames.add(fieldName);
            break;
          case BOOLEAN:
          case STRING:
            _columnNames.add(fieldName);
            _columnToValues.put(fieldName, new HashSet<String>());
            _singleValueColumnNames.add(fieldName);
            break;
          default:
            LOGGER.warn("Ignoring field {} of type {}", fieldName, fieldType);
            break;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Load avro data into storage.
    for (File avroFile : avroFiles) {
      addAvroData(avroFile);
    }

    // Ignore multi-value columns with too many elements.
    prepareToGenerateQueries();
  }

  /**
   * Helper method to read in an avro file and add the data to the storage.
   *
   * @param avroFile avro file.
   */
  private void addAvroData(File avroFile) {
    // Read in records and update the values stored.
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    try (DataFileReader<GenericRecord> fileReader = new DataFileReader<>(avroFile, datumReader)) {
      for (GenericRecord genericRecord : fileReader) {
        for (String columnName : _columnNames) {
          Set<String> values = _columnToValues.get(columnName);

          // Turn the avro value into a valid SQL String token.
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
   * Helper method to store an avro value into the valid SQL String value set.
   *
   * @param valueSet value set.
   * @param avroValue avro value.
   */
  private static void storeAvroValueIntoValueSet(Set<String> valueSet, Object avroValue) {
    if (avroValue instanceof Number) {
      valueSet.add(avroValue.toString());
    } else {
      valueSet.add("'" + avroValue.toString().replace("'", "''") + "'");
    }
  }

  /**
   * Helper method to finish initialization of the query generator, removing multi-value columns with too many elements
   * and dumping storage into the final map from column name to list of column values.
   * Called after all avro data loaded.
   */
  private void prepareToGenerateQueries() {
    Iterator<String> columnNameIterator = _columnNames.iterator();
    while (columnNameIterator.hasNext()) {
      String columnName = columnNameIterator.next();

      // Remove multi-value columns with more than MAX_ELEMENTS_FOR_MULTI_VALUE elements.
      Integer maxNumElements = _multiValueColumnMaxNumElements.get(columnName);
      if (maxNumElements != null && maxNumElements > BaseClusterIntegrationTest.MAX_ELEMENTS_FOR_MULTI_VALUE) {
        LOGGER.debug("Ignoring column {} with max number of {} elements", columnName, maxNumElements);
        columnNameIterator.remove();
        _multiValueColumnMaxNumElements.remove(columnName);
      } else {
        _columnToValueList.put(columnName, new ArrayList<>(_columnToValues.get(columnName)));
      }
    }

    // Free the other copy of the data.
    _columnToValues.clear();
  }

  /**
   * Set whether to skip predicates on multi-value columns.
   *
   * @param skipMultiValuePredicates boolean value.
   */
  public void setSkipMultiValuePredicates(boolean skipMultiValuePredicates) {
    _skipMultiValuePredicates = skipMultiValuePredicates;
  }

  /**
   * Helper method to pick a random value from the values list.
   *
   * @param list values list.
   * @param <T> type of the value.
   * @return value randomly picked.
   */
  private <T> T pickRandom(List<T> list) {
    return list.get(RANDOM.nextInt(list.size()));
  }

  /**
   * Helper method to join several String elements with ' '.
   *
   * @param elements elements to be joined.
   * @return joined String.
   */
  private static String joinWithSpaces(String... elements) {
    return StringUtils.join(elements, ' ');
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
   * Selection query.
   */
  private class SelectionQuery implements Query {
    private final List<String> _projectionColumns;
    private final QueryFragment _predicate;
    private final QueryFragment _orderBy;
    private final QueryFragment _limit;

    /**
     * Constructor for SelectionQuery.
     *
     * @param projectionColumns projection columns.
     * @param orderBy order by fragment.
     * @param predicate predicate fragment.
     * @param limit limit fragment.
     */
    public SelectionQuery(List<String> projectionColumns, QueryFragment orderBy, QueryFragment predicate,
        QueryFragment limit) {
      _projectionColumns = projectionColumns;
      _orderBy = orderBy;
      _predicate = predicate;
      _limit = limit;
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public String generatePql() {
      return joinWithSpaces("SELECT", StringUtils.join(_projectionColumns, ", "), "FROM", _pqlTableName,
          _predicate.generatePql(), _orderBy.generatePql(), _limit.generatePql());
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public List<String> generateH2Sql() {
      List<String> h2ProjectionColumns = new ArrayList<>();
      for (String projectionColumn : _projectionColumns) {
        if (_multiValueColumnMaxNumElements.containsKey(projectionColumn)) {
          // Multi-value column.

          for (int i = 0; i < BaseClusterIntegrationTest.MAX_ELEMENTS_FOR_MULTI_VALUE; i++) {
            h2ProjectionColumns.add(projectionColumn + "__MV" + i);
          }
        } else {
          // Single-value column.

          h2ProjectionColumns.add(projectionColumn);
        }
      }
      return Collections.singletonList(joinWithSpaces("SELECT", StringUtils.join(h2ProjectionColumns, ", "), "FROM",
          _h2TableName, _predicate.generateH2Sql(), _orderBy.generateH2Sql(), _limit.generateH2Sql()));
    }
  }

  /**
   * Aggregation query.
   */
  private class AggregationQuery implements Query {
    private List<String> _aggregateColumnsAndFunctions;
    private QueryFragment _predicate;
    private Set<String> _groupColumns;
    private QueryFragment _top;

    /**
     * Constructor for AggregationQuery.
     *
     * @param aggregateColumnsAndFunctions aggregation functions.
     * @param predicate predicate fragment.
     * @param groupColumns group-by columns.
     * @param top top fragment.
     */
    public AggregationQuery(List<String> aggregateColumnsAndFunctions, QueryFragment predicate,
        Set<String> groupColumns, QueryFragment top) {
      _aggregateColumnsAndFunctions = aggregateColumnsAndFunctions;
      _predicate = predicate;
      _groupColumns = groupColumns;
      _top = top;
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public String generatePql() {
      String queryBody =
          joinWithSpaces("SELECT", StringUtils.join(_aggregateColumnsAndFunctions, ", "), "FROM", _pqlTableName,
              _predicate.generatePql());

      if (_groupColumns.isEmpty()) {
        return queryBody + " " + _top.generatePql();
      } else {
        return queryBody + " GROUP BY " + StringUtils.join(_groupColumns, ", ") + " " + _top.generatePql();
      }
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
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
          queries.add(joinWithSpaces("SELECT", groupByColumns + ",", aggregateColumnAndFunction, "FROM", _h2TableName,
              _predicate.generateH2Sql(), "GROUP BY", groupByColumns, _top.generateH2Sql()));
        }
      }

      return queries;
    }
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
   * QueryFragment interface with capability of generating PQL and H2 SQL query fragment.
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
   * Most basic query fragment.
   */
  private class StringQueryFragment implements QueryFragment {
    String _pql;
    String _sql;

    /**
     * Constructor for StringQueryFragment with same PQL and H2 SQL query fragment.
     *
     * @param pql PQL (H2 SQL) query fragment.
     */
    StringQueryFragment(String pql) {
      _pql = pql;
      _sql = pql;
    }

    /**
     * Constructor for StringQueryFragment with different PQL and H2 SQL query fragment.
     *
     * @param pql PQL query fragment.
     * @param sql H2 SQL query fragment.
     */
    StringQueryFragment(String pql, String sql) {
      _pql = pql;
      _sql = sql;
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public String generatePql() {
      return _pql;
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public String generateH2Sql() {
      return _sql;
    }
  }

  /**
   * Limit query fragment for selection queries.
   *
   * SELECT ... FROM ... WHERE ... 'LIMIT ...'.
   */
  private class LimitQueryFragment extends StringQueryFragment {
    LimitQueryFragment(int limit) {
      super("LIMIT " + limit, "LIMIT " + BaseClusterIntegrationTest.MAX_COMPARISON_LIMIT);
    }
  }

  /**
   * Top query fragment for aggregation queries.
   *
   * SELECT ... FROM ... WHERE ... 'TOP ...'.
   */
  private class TopQueryFragment extends StringQueryFragment {
    TopQueryFragment(int top) {
      super("TOP " + top, "LIMIT " + BaseClusterIntegrationTest.MAX_COMPARISON_LIMIT);
    }
  }

  /**
   * Order by query fragment for aggregation queries.
   *
   * SELECT ... FROM ... WHERE ... 'ORDER BY ...'
   */
  private class OrderByQueryFragment extends StringQueryFragment {
    OrderByQueryFragment(Set<String> columns) {
      super(columns.isEmpty() ? "" : "ORDER BY " + StringUtils.join(columns, ", "));
    }
  }

  /**
   * Predicate query fragment.
   *
   * SELECT ... FROM ... 'WHERE ...'
   */
  private class PredicateQueryFragment implements QueryFragment {
    List<QueryFragment> _predicates;
    List<QueryFragment> _operators;

    /**
     * Constructor for PredicateQueryFragment.
     *
     * @param predicates predicates.
     * @param operators operators between predicates.
     */
    PredicateQueryFragment(List<QueryFragment> predicates, List<QueryFragment> operators) {
      _predicates = predicates;
      _operators = operators;
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public String generatePql() {
      if (_predicates.isEmpty()) {
        return "";
      } else {
        StringBuilder pql = new StringBuilder("WHERE ");

        // One less than the number of predicates.
        int operatorCount = _operators.size();
        for (int i = 0; i < operatorCount; i++) {
          pql.append(_predicates.get(i).generatePql())
              .append(' ')
              .append(_operators.get(i).generatePql())
              .append(' ');
        }
        pql.append(_predicates.get(operatorCount).generatePql());

        return pql.toString();
      }
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public String generateH2Sql() {
      if (_predicates.isEmpty()) {
        return "";
      } else {
        StringBuilder sql = new StringBuilder("WHERE ");

        // One less than the number of predicates.
        int operatorCount = _operators.size();
        for (int i = 0; i < operatorCount; i++) {
          sql.append(_predicates.get(i).generateH2Sql())
              .append(' ')
              .append(_operators.get(i).generateH2Sql())
              .append(' ');
        }
        sql.append(_predicates.get(operatorCount).generateH2Sql());

        return sql.toString();
      }
    }
  }

  /**
   * Helper method to generate a predicate query fragment.
   *
   * @return generated predicate query fragment.
   */
  private PredicateQueryFragment generatePredicate() {
    // Generate at most MAX_PREDICATE_COUNT predicates.
    int predicateCount = RANDOM.nextInt(MAX_PREDICATE_COUNT + 1);

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
   * QueryGenerationStrategy interface with capability of generating query using specific strategy.
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
   * Strategy to generate selection queries.
   *
   * SELECT a, b FROM table WHERE a = 'foo' AND b = 'bar' ORDER BY c LIMIT 10
   */
  private class SelectionQueryGenerationStrategy implements QueryGenerationStrategy {

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public Query generateQuery() {
      // Select at most MAX_SELECTION_COLUMNS columns.
      int projectionColumnCount = Math.min(RANDOM.nextInt(MAX_SELECTION_COLUMNS) + 1, _columnNames.size());
      Set<String> projectionColumns = new HashSet<>();
      while (projectionColumns.size() < projectionColumnCount) {
        projectionColumns.add(pickRandom(_columnNames));
      }

      // Select at most MAX_ORDER_BY_COLUMNS columns for ORDER BY clause.
      int orderByColumnCount = Math.min(RANDOM.nextInt(MAX_ORDER_BY_COLUMNS + 1), _singleValueColumnNames.size());
      Set<String> orderByColumns = new HashSet<>();
      while (orderByColumns.size() < orderByColumnCount) {
        orderByColumns.add(pickRandom(_singleValueColumnNames));
      }

      // Generate a predicate.
      QueryFragment predicate = generatePredicate();

      // Generate a result limit of at most MAX_RESULT_LIMIT columns for ORDER BY clause.
      int resultLimit = RANDOM.nextInt(MAX_RESULT_LIMIT + 1);
      LimitQueryFragment limit = new LimitQueryFragment(resultLimit);

      return new SelectionQuery(new ArrayList<>(projectionColumns), new OrderByQueryFragment(orderByColumns),
          predicate, limit);
    }
  }

  /**
   * Strategy to generate aggregation queries.
   *
   * SELECT SUM(a), MAX(b) FROM table WHERE a = 'foo' AND b = 'bar' GROUP BY c TOP 10
   */
  private class AggregationQueryGenerationStrategy implements QueryGenerationStrategy {

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public Query generateQuery() {
      // Generate at most MAX_AGGREGATE_COLUMNS columns on which to aggregate, map 0 to 'COUNT(*)'.
      int aggregationColumnCount = RANDOM.nextInt(MAX_AGGREGATE_COLUMNS + 1);
      Set<String> aggregationColumnsAndFunctions = new HashSet<>();
      if (aggregationColumnCount == 0) {
        aggregationColumnsAndFunctions.add("COUNT(*)");
      } else {
        while (aggregationColumnsAndFunctions.size() < aggregationColumnCount) {
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
          aggregationColumnsAndFunctions.add(aggregationFunction + "(" + aggregationColumn + ")");
        }
      }

      // Generate a predicate.
      QueryFragment predicate = generatePredicate();

      // Generate at most MAX_GROUP_COLUMNS columns on which to group.
      int groupColumnCount = Math.min(RANDOM.nextInt(MAX_GROUP_COLUMNS + 1), _singleValueColumnNames.size());
      Set<String> groupColumns = new HashSet<>();
      while (groupColumns.size() < groupColumnCount) {
        groupColumns.add(pickRandom(_singleValueColumnNames));
      }

      // Generate a result limit of at most MAX_RESULT_LIMIT.
      TopQueryFragment top = new TopQueryFragment(RANDOM.nextInt(MAX_RESULT_LIMIT + 1));

      return new AggregationQuery(new ArrayList<>(aggregationColumnsAndFunctions), predicate, groupColumns, top);
    }
  }

  /**
   * PredicateGenerator interface with capability of generating a predicate query fragment on a column.
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
   * Generator for single-value column comparison predicate query fragment.
   */
  private class SingleValueComparisonPredicateGenerator implements PredicateGenerator {

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public QueryFragment generatePredicate(String columnName) {
      String columnValue = pickRandom(_columnToValueList.get(columnName));
      String comparisonOperator = pickRandom(COMPARISON_OPERATORS);
      return new StringQueryFragment(joinWithSpaces(columnName, comparisonOperator, columnValue));
    }
  }

  /**
   * Generator for single-value column IN predicate query fragment.
   */
  private class SingleValueInPredicateGenerator implements PredicateGenerator {

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public QueryFragment generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);

      int numValues = Math.min(RANDOM.nextInt(MAX_IN_CLAUSE_NUM_VALUES) + 1, columnValues.size());
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
   * Generator for single-value column BETWEEN predicate query fragment.
   */
  private class SingleValueBetweenPredicateGenerator implements PredicateGenerator {

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public QueryFragment generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);
      String leftValue = pickRandom(columnValues);
      String rightValue = pickRandom(columnValues);
      return new StringQueryFragment(columnName + " BETWEEN " + leftValue + " AND " + rightValue);
    }
  }

  /**
   * Generator for multi-value column comparison predicate query fragment.
   * DO NOT SUPPORT 'NOT EQUAL'.
   */
  private class MultiValueComparisonPredicateGenerator implements PredicateGenerator {

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public QueryFragment generatePredicate(String columnName) {
      String columnValue = pickRandom(_columnToValueList.get(columnName));
      String comparisonOperator = pickRandom(COMPARISON_OPERATORS);

      // Not equal works differently on multi-value, so avoid '<>' comparison.
      while (comparisonOperator.equals("<>")) {
        comparisonOperator = pickRandom(COMPARISON_OPERATORS);
      }

      List<String> h2ComparisonClauses = new ArrayList<>(BaseClusterIntegrationTest.MAX_ELEMENTS_FOR_MULTI_VALUE);
      for (int i = 0; i < BaseClusterIntegrationTest.MAX_ELEMENTS_FOR_MULTI_VALUE; i++) {
        h2ComparisonClauses.add(joinWithSpaces(columnName + "__MV" + i, comparisonOperator, columnValue));
      }

      return new StringQueryFragment(joinWithSpaces(columnName, comparisonOperator, columnValue),
          "(" + StringUtils.join(h2ComparisonClauses, " OR ") + ")");
    }
  }

  /**
   * Generator for multi-value column IN predicate query fragment.
   * DO NOT SUPPORT 'NOT IN'.
   */
  private class MultiValueInPredicateGenerator implements PredicateGenerator {

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public QueryFragment generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);

      int numValues = Math.min(RANDOM.nextInt(MAX_IN_CLAUSE_NUM_VALUES) + 1, columnValues.size());
      Set<String> values = new HashSet<>();
      while (values.size() < numValues) {
        values.add(pickRandom(columnValues));
      }
      String inValues = StringUtils.join(values, ", ");

      List<String> h2InClauses = new ArrayList<>(BaseClusterIntegrationTest.MAX_ELEMENTS_FOR_MULTI_VALUE);
      for (int i = 0; i < BaseClusterIntegrationTest.MAX_ELEMENTS_FOR_MULTI_VALUE; i++) {
        h2InClauses.add(columnName + "__MV" + i + " IN (" + inValues + ")");
      }

      return new StringQueryFragment(columnName + " IN (" + inValues + ")",
          "(" + StringUtils.join(h2InClauses, " OR ") + ")");
    }
  }

  /**
   * Generator for multi-value column BETWEEN predicate query fragment.
   */
  private class MultiValueBetweenPredicateGenerator implements PredicateGenerator {

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public QueryFragment generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);
      String leftValue = pickRandom(columnValues);
      String rightValue = pickRandom(columnValues);

      List<String> h2ComparisonClauses = new ArrayList<>(BaseClusterIntegrationTest.MAX_ELEMENTS_FOR_MULTI_VALUE);
      for (int i = 0; i < BaseClusterIntegrationTest.MAX_ELEMENTS_FOR_MULTI_VALUE; i++) {
        h2ComparisonClauses.add(columnName + "__MV" + i + " BETWEEN " + leftValue + " AND " + rightValue);
      }

      return new StringQueryFragment(columnName + " BETWEEN " + leftValue + " AND " + rightValue,
          "(" + StringUtils.join(h2ComparisonClauses, " OR ") + ")");
    }
  }

  /**
   * Sample main class for the query generator.
   *
   * @param args arguments.
   */
  public static void main(String[] args) {
    File avroFile = new File("pinot-integration-tests/src/test/resources/On_Time_On_Time_Performance_2014_1.avro");
    QueryGenerator qg = new QueryGenerator(Collections.singletonList(avroFile), "whatever", "whatever");
    for (int i = 0; i < 100; i++) {
      System.out.println(qg.generateQuery().generatePql());
    }
  }
}
