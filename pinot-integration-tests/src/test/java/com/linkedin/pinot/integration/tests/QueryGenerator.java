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
package com.linkedin.pinot.integration.tests;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;

import com.linkedin.pinot.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class to generate random SQL queries based on an Avro file.
 *
 */
public class QueryGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryGenerator.class);
  private Map<String, SortedSet<String>> _columnToValues = new HashMap<String, SortedSet<String>>();
  private Map<String, List<String>> _columnToValueList = new HashMap<String, List<String>>();
  private List<String> _columnNames = new ArrayList<String>();
  private List<String> _nonMultivalueColumnNames = new ArrayList<String>();
  private List<String> _nonMultivalueNumericalColumnNames = new ArrayList<String>();
  private Map<String, Integer> _multivalueColumnCardinality = new HashMap<String, Integer>();
  private List<QueryGenerationStrategy> _queryGenerationStrategies =
      Collections.<QueryGenerationStrategy>singletonList(new AggregationQueryGenerationStrategy());
     /* Arrays.asList(
      new SelectionQueryGenerationStrategy(), new AggregationQueryGenerationStrategy()); */
  private List<String> _booleanOperators = Arrays.asList("OR", "AND");
  private List<PredicateGenerator> _predicateGenerators = Arrays.asList(new ComparisonOperatorPredicateGenerator(),
      new InPredicateGenerator(), new BetweenPredicateGenerator());
  private static final int MAX_MULTIVALUE_CARDINALITY = 5;
  private static final long RANDOM_SEED = -1L;
  private static final Random RANDOM;
  private final String _pqlTableName;
  private final String _h2TableName;
  private boolean skipMultivaluePredicates;

  static {
    if (RANDOM_SEED == -1L) {
      RANDOM = new Random();
    } else {
      RANDOM = new Random(RANDOM_SEED);
    }
  }

  public QueryGenerator(final List<File> avroFiles, final String pqlTableName, String h2TableName) {
    _pqlTableName = pqlTableName;
    _h2TableName = h2TableName;
    // Read schema and initialize storage
    File schemaAvroFile = avroFiles.get(0);
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> fileReader = null;
    try {
      fileReader = new DataFileReader<GenericRecord>(schemaAvroFile, datumReader);

      Schema schema = fileReader.getSchema();
      for (Schema.Field field : schema.getFields()) {
        Schema fieldSchema = field.schema();
        Schema.Type fieldType = fieldSchema.getType();
        String fieldName = field.name();

        switch (fieldType) {
          case UNION:
            List<Schema> unionTypes = fieldSchema.getTypes();

            _columnNames.add(fieldName);
            _columnToValues.put(fieldName, new TreeSet<String>());

            // We assume here that we can only have strings and numerical values, no arrays, unions, etc.
            if (unionTypes.get(0).getType() == Schema.Type.ARRAY) {
              _columnNames.add(fieldName);
              _multivalueColumnCardinality.put(fieldName, 0);
            } else if (unionTypes.get(0).getType() != Schema.Type.STRING) {
              _nonMultivalueNumericalColumnNames.add(fieldName);
            }
            break;
          case ARRAY:
            _columnNames.add(fieldName);
            _multivalueColumnCardinality.put(fieldName, 0);
            break;
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
            _columnNames.add(fieldName);
            _columnToValues.put(fieldName, new TreeSet<String>());
            _nonMultivalueNumericalColumnNames.add(fieldName);
            break;
          case RECORD:
            LOGGER.warn("Ignoring field {} of type RECORD", fieldName);
            break;
          default:
            LOGGER.warn("Ignoring field {} of type {}", fieldName, fieldType);
            break;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      IOUtils.closeQuietly(fileReader);
    }

    for (File avroFile : avroFiles) {
      addAvroData(avroFile);
    }

    prepareToGenerateQueries();
  }

  /**
   * Reads in an avro file to add it to the set of data that can be queried
   */
  public void addAvroData(File avroFile) {
    // Read in records and update the values stored
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> fileReader = null;
    try {
      fileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);

      for (GenericRecord genericRecord : fileReader) {
        for (String columnName : _columnNames) {
          SortedSet<String> values = _columnToValues.get(columnName);
          if (values == null) {
            values = new TreeSet<String>();
            _columnToValues.put(columnName, values);
          }

          Object avroValue = genericRecord.get(columnName);

          // Turn the value into a valid SQL token
          if (avroValue == null) {
            continue;
          } else {
            if (_multivalueColumnCardinality.containsKey(columnName)) {
              GenericData.Array array = (GenericData.Array) avroValue;
              Integer storedCardinality = _multivalueColumnCardinality.get(columnName);
              if (storedCardinality < array.size()) {
                _multivalueColumnCardinality.put(columnName, array.size());
              }
              for (Object element : array) {
                storeAvroValueIntoValueSet(values, element);
              }
            } else {
              storeAvroValueIntoValueSet(values, avroValue);
            }
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      IOUtils.closeQuietly(fileReader);
    }
  }

  private void storeAvroValueIntoValueSet(Set<String> valueSet, Object avroValue) {
    if (avroValue instanceof Number) {
      valueSet.add(avroValue.toString());
    } else {
      valueSet.add("'" + avroValue.toString().replace("'", "''") + "'");
    }
  }

  /**
   * Finishes initialization of the query generator, once all Avro data has been loaded.
   */
  public void prepareToGenerateQueries() {
    for (String columnName : _columnNames) {
      _columnToValueList.put(columnName, new ArrayList<String>(_columnToValues.get(columnName)));
      if (!_multivalueColumnCardinality.containsKey(columnName)) {
        _nonMultivalueColumnNames.add(columnName);
      }
    }

    for (Map.Entry<String, Integer> entry : _multivalueColumnCardinality.entrySet()) {
      String columnName = entry.getKey();
      Integer columnCardinality = entry.getValue();
      if (MAX_MULTIVALUE_CARDINALITY < columnCardinality) {
        LOGGER.warn("Ignoring column {} with a cardinality of {}, exceeds maximum multivalue column cardinality of {}",
            columnName, columnCardinality, MAX_MULTIVALUE_CARDINALITY);
        _columnNames.remove(columnName);
      }
    }

    // Free the other copy of the data
    _columnToValues = null;
  }

  public void setSkipMultivaluePredicates(boolean skipMultivaluePredicates) {
    this.skipMultivaluePredicates = skipMultivaluePredicates;
  }

  private interface QueryFragment {
    String generatePql();

    String generateH2Sql();
  }

  public interface Query {
    String generatePql();

    List<String> generateH2Sql();
  }

  private <T> T pickRandom(List<T> items) {
    return items.get(RANDOM.nextInt(items.size()));
  }

  public Query generateQuery() {
    return pickRandom(_queryGenerationStrategies).generateQuery();
  }

  private interface QueryGenerationStrategy {
    Query generateQuery();
  }

  private class StringQueryFragment implements QueryFragment {
    private String pql;
    private String hql;

    private StringQueryFragment(String querySql) {
      pql = querySql;
      hql = querySql;
    }

    public StringQueryFragment(String pql, String hql) {
      this.pql = pql;
      this.hql = hql;
    }

    @Override
    public String generatePql() {
      return pql;
    }

    @Override
    public String generateH2Sql() {
      return hql;
    }
  }

  private class LimitQueryFragment extends StringQueryFragment {
    private LimitQueryFragment(int limit) {
      super(0 <= limit ? "LIMIT " + limit : "", "LIMIT 10000");
    }
  }

  private class OrderByQueryFragment extends StringQueryFragment {
    private OrderByQueryFragment(Set<String> columns) {
      super(columns.isEmpty() ? "" : "ORDER BY " + joinWithCommas(new ArrayList<String>(columns)));
    }
  }

  private class PredicateQueryFragment implements QueryFragment {
    List<QueryFragment> _predicates;
    List<QueryFragment> _operators;

    public PredicateQueryFragment(List<QueryFragment> predicates, List<QueryFragment> operators) {
      _predicates = predicates;
      _operators = operators;
    }

    @Override
    public String generatePql() {
      if (_predicates.isEmpty()) {
        return "";
      } else if (_predicates.size() == 1) {
        return " WHERE " + _predicates.get(0).generatePql();
      }

      String pql = " WHERE ";

      // One less than the number of predicates
      int operatorCount = _operators.size();
      for (int i = 0; i < operatorCount; i++) {
        pql += _predicates.get(i).generatePql() + " " + _operators.get(i).generatePql() + " ";
      }

      pql += _predicates.get(operatorCount).generatePql();
      return pql;
    }

    @Override
    public String generateH2Sql() {
      if (_predicates.isEmpty()) {
        return "";
      } else if (_predicates.size() == 1) {
        return " WHERE " + _predicates.get(0).generateH2Sql();
      }

      String h2sql = " WHERE ";

      // One less than the number of predicates
      int operatorCount = _operators.size();
      for (int i = 0; i < operatorCount; i++) {
        h2sql += _predicates.get(i).generateH2Sql() + " " + _operators.get(i).generateH2Sql() + " ";
      }

      h2sql += _predicates.get(operatorCount).generateH2Sql();
      return h2sql;
    }
  }

  private QueryFragment generatePredicate() {
    int predicateCount = RANDOM.nextInt(3);

    List<QueryFragment> predicates = new ArrayList<QueryFragment>();
    for (int i = 0; i < predicateCount; i++) {
      String columnName = pickRandom(_columnNames);
      if (!_columnToValueList.get(columnName).isEmpty()) {
        if (!_multivalueColumnCardinality.containsKey(columnName)) {
          predicates.add(pickRandom(_predicateGenerators).generatePredicate(columnName));
        } else {
          if (!skipMultivaluePredicates) {
            predicates.add(new MultivaluePredicateGenerator().generatePredicate(columnName));
          }
        }
      }
    }

    if (predicates.size() < 2) {
      return new PredicateQueryFragment(predicates, Collections.<QueryFragment> emptyList());
    }

    // Join predicates with ANDs and ORs
    List<QueryFragment> operators = new ArrayList<QueryFragment>(predicates.size() - 1);
    for (int i = 1; i < predicates.size(); i++) {
      operators.add(new StringQueryFragment(pickRandom(_booleanOperators)));
    }

    return new PredicateQueryFragment(predicates, operators);
  }

  /**
   * Queries similar to SELECT blah FROM blah WHERE ... LIMIT blah
   */
  private class SelectionQueryGenerationStrategy implements QueryGenerationStrategy {
    @Override
    public Query generateQuery() {
      // Select 0-9 columns, map 0 columns to SELECT *
      Set<String> projectionColumns = new HashSet<String>();
      int projectionColumnCount = RANDOM.nextInt(3);
      for (int i = 0; i < projectionColumnCount; i++) {
        projectionColumns.add(pickRandom(_nonMultivalueColumnNames));
      }
      if (projectionColumns.isEmpty()) {
        projectionColumns.add("*");
      }

      // Select 0-9 columns for ORDER BY clause
      Set<String> orderByColumns = new HashSet<String>();
      int orderByColumnCount = RANDOM.nextInt(1);
      for (int i = 0; i < orderByColumnCount; i++) {
        orderByColumns.add(pickRandom(_nonMultivalueColumnNames));
      }

      // Generate a predicate
      QueryFragment predicate = generatePredicate();

      // Generate a result limit between 0 and 5000 as negative numbers mean no limit
      int resultLimit = RANDOM.nextInt(30) + 1;
      LimitQueryFragment limit = new LimitQueryFragment(resultLimit);

      return new SelectionQuery(projectionColumns, new OrderByQueryFragment(orderByColumns), predicate, limit);
    }
  }

  private class ValueQueryFragment implements QueryFragment {
    private String pql;
    private String hql;

    public ValueQueryFragment(String value) {
      hql = value;

      if (!value.startsWith("'")) {
        pql = "'" + value + "'";
      } else {
        pql = value;
      }
    }

    @Override
    public String generatePql() {
      return pql;
    }

    @Override
    public String generateH2Sql() {
      return hql;
    }
  }

  private class SelectionQuery implements Query {
    private final List<String> _projectionColumns;
    private final QueryFragment _orderBy;
    private final QueryFragment _predicate;
    private final QueryFragment _limit;

    public SelectionQuery(Set<String> projectionColumns, QueryFragment orderBy, QueryFragment predicate,
        QueryFragment limit) {
      _projectionColumns = new ArrayList<String>(projectionColumns);
      _orderBy = orderBy;
      _predicate = predicate;
      _limit = limit;
    }

    @Override
    public String generatePql() {
      return joinWithSpaces("SELECT", joinWithCommas(_projectionColumns), "FROM", _pqlTableName,
          _predicate.generatePql(), _orderBy.generatePql(), _limit.generatePql());
    }

    @Override
    public List<String> generateH2Sql() {
      return Collections.singletonList(joinWithSpaces("SELECT", joinWithCommas(_projectionColumns), "FROM",
          _h2TableName, _predicate.generateH2Sql(), _orderBy.generateH2Sql(), _limit.generateH2Sql()));
    }
  }

  private static String joinWithCommas(List<String>... elements) {
    List<String> joinedList = new ArrayList<String>();
    for (List<String> element : elements) {
      joinedList.addAll(element);
    }

    return StringUtil.join(", ", joinedList.toArray(new String[joinedList.size()]));
  }

  private static String joinWithSpaces(String... elements) {
    return StringUtil.join(" ", elements);
  }

  private class AggregationQuery implements Query {
    private List<String> _groupColumns;
    private List<String> _aggregateColumnsAndFunctions;
    private QueryFragment _predicate;
    private QueryFragment _limit;

    public AggregationQuery(List<String> groupColumns, List<String> aggregateColumnsAndFunctions,
        QueryFragment predicate, QueryFragment limit) {
      this._groupColumns = groupColumns;
      this._aggregateColumnsAndFunctions = aggregateColumnsAndFunctions;
      this._predicate = predicate;
      _limit = limit;
    }

    @Override
    public String generatePql() {
      // Unlike SQL, PQL doesn't expect the group columns in select statements
      String queryBody =
          joinWithSpaces("SELECT", joinWithCommas(_aggregateColumnsAndFunctions), "FROM", _pqlTableName,
              _predicate.generatePql());

      if (_groupColumns.isEmpty()) {
        return queryBody + " " + _limit.generatePql();
      } else {
        return queryBody + " GROUP BY " + joinWithCommas(_groupColumns) + " " + _limit.generatePql();
      }
    }

    @Override
    public List<String> generateH2Sql() {
      List<String> queries = new ArrayList<String>();
      if (_groupColumns.isEmpty()) {
        for (String aggregateColumnAndFunction : _aggregateColumnsAndFunctions) {
          queries.add(joinWithSpaces("SELECT", aggregateColumnAndFunction, "FROM", _h2TableName,
              _predicate.generateH2Sql(), _limit.generateH2Sql()));
        }
      } else {
        for (String aggregateColumnAndFunction : _aggregateColumnsAndFunctions) {
          if (aggregateColumnAndFunction.startsWith("avg(")) {
            aggregateColumnAndFunction = aggregateColumnAndFunction.replace("avg(", "avg(cast(").replace(")", " as double))");
          }
          queries.add(joinWithSpaces("SELECT", joinWithCommas(_groupColumns) + ",", aggregateColumnAndFunction, "FROM",
              _h2TableName, _predicate.generateH2Sql(), "GROUP BY", joinWithCommas(_groupColumns), _limit.generateH2Sql()));
        }
      }
      return queries;
    }
  }

  /**
   * Queries similar to SELECT foo, SUM(bar) FROM blah WHERE ... GROUP BY foo
   */
  private class AggregationQueryGenerationStrategy implements QueryGenerationStrategy {
    private final List<String> aggregationFunctions = Arrays.asList("sum", "min", "max", "count", "avg");

    @Override
    public Query generateQuery() {
      // Generate 0-3 columns on which to group
      Set<String> groupColumns = new HashSet<String>();
      int groupColumnCount = RANDOM.nextInt(2) + 1;
      for (int i = 0; i < groupColumnCount; i++) {
        groupColumns.add(pickRandom(_nonMultivalueColumnNames));
      }

      // Generate a disjoint set of 0-3 columns on which to aggregate
      int aggregationColumnCount = RANDOM.nextInt(2) + 1;
      Set<String> aggregationColumns = new HashSet<String>();
      for (int i = 0; i < aggregationColumnCount; i++) {
        String randomColumn = pickRandom(_nonMultivalueNumericalColumnNames);
        if (!groupColumns.contains(randomColumn)) {
          aggregationColumns.add(randomColumn);
        }
      }
      List<String> aggregationColumnsAndFunctions = new ArrayList<String>();
      if (aggregationColumns.isEmpty()) {
        aggregationColumnsAndFunctions.add("COUNT(*)");
      } else {
        for (String aggregationColumn : aggregationColumns) {
          int aggregationFunctionCount = RANDOM.nextInt(aggregationFunctions.size()) + 1;
          for (int i = 0; i < aggregationFunctionCount; i++) {
            aggregationColumnsAndFunctions.add(pickRandom(aggregationFunctions) + "(" + aggregationColumn + ")");
          }
        }
      }

      // FIXME Always only one aggregation function
      aggregationColumnsAndFunctions = Collections.singletonList(aggregationColumnsAndFunctions.get(0));

      // Generate a predicate
      QueryFragment predicate = generatePredicate();

      // Generate a result limit between 0 and 30 as negative numbers mean no limit
      int resultLimit = RANDOM.nextInt(30);
      LimitQueryFragment limit = new LimitQueryFragment(resultLimit);

      return new AggregationQuery(new ArrayList<String>(groupColumns), aggregationColumnsAndFunctions, predicate, limit);
    }
  }

  private interface PredicateGenerator {
    QueryFragment generatePredicate(String columnName);
  }

  private class ComparisonOperatorPredicateGenerator implements PredicateGenerator {
    private List<String> _comparisonOperators = Arrays.asList("=", "<>", "<", ">", "<=", ">=");

    @Override
    public QueryFragment generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);
      ValueQueryFragment value = new ValueQueryFragment(pickRandom(columnValues));
      String comparisonOperator = pickRandom(_comparisonOperators);
      return new StringQueryFragment(
          columnName + " " + comparisonOperator + " " + value.generatePql(),
          columnName + " " + comparisonOperator + " " + value.generateH2Sql());
    }
  }

  private class InPredicateGenerator implements PredicateGenerator {
    @Override
    public QueryFragment generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);

      int inValueCount = RANDOM.nextInt(10) + 1;
      List<String> pqlInValueList = new ArrayList<String>(inValueCount);
      List<String> h2InValueList = new ArrayList<String>(inValueCount);
      for (int i = 0; i < inValueCount; i++) {
        ValueQueryFragment value = new ValueQueryFragment(pickRandom(columnValues));
        pqlInValueList.add(value.generatePql());
        h2InValueList.add(value.generateH2Sql());
      }
      String pqlInValues = StringUtil.join(", ", pqlInValueList.toArray(new String[pqlInValueList.size()]));
      String h2InValues = StringUtil.join(", ", h2InValueList.toArray(new String[h2InValueList.size()]));

      boolean notIn = RANDOM.nextBoolean();

      if (notIn) {
        return new StringQueryFragment(
            columnName + " NOT IN (" + pqlInValues + ")",
            columnName + " NOT IN (" + h2InValues + ")");
      } else {
        return new StringQueryFragment(
            columnName + " IN (" + pqlInValues + ")",
            columnName + " IN (" + h2InValues + ")");
      }
    }
  }

  private class MultivaluePredicateGenerator implements PredicateGenerator {
    @Override
    public QueryFragment generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);

      int inValueCount = RANDOM.nextInt(10) + 1;
      List<String> pqlInValueList = new ArrayList<String>(inValueCount);
      List<String> h2InValueList = new ArrayList<String>(inValueCount);
      for (int i = 0; i < inValueCount; i++) {
        ValueQueryFragment value = new ValueQueryFragment(pickRandom(columnValues));
        pqlInValueList.add(value.generatePql());
        h2InValueList.add(value.generateH2Sql());
      }
      String pqlInValues = StringUtil.join(", ", pqlInValueList.toArray(new String[pqlInValueList.size()]));
      String h2InValues = StringUtil.join(", ", h2InValueList.toArray(new String[h2InValueList.size()]));
      String[] h2InClauses = new String[MAX_MULTIVALUE_CARDINALITY];
      for (int i = 0; i < MAX_MULTIVALUE_CARDINALITY; i++) {
        h2InClauses[i] = columnName + i + " IN (" + h2InValues + ")";
      }
      String h2QueryFragment = "(" + StringUtil.join(" OR ", h2InClauses) + ")";

      return new StringQueryFragment(
          columnName + " IN (" + pqlInValues + ")",
          h2QueryFragment);
    }
  }

  private class BetweenPredicateGenerator implements PredicateGenerator {
    @Override
    public QueryFragment generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);
      ValueQueryFragment leftValue = new ValueQueryFragment(pickRandom(columnValues));
      ValueQueryFragment rightValue = new ValueQueryFragment(pickRandom(columnValues));
      return new StringQueryFragment(
          columnName + " BETWEEN " + leftValue.generatePql() + " AND " + rightValue.generatePql(),
          columnName + " BETWEEN " + leftValue.generateH2Sql() + " AND " + rightValue.generateH2Sql());
    }
  }

  public static void main(String[] args) {
    File avroFile = new File("pinot-integration-tests/src/test/resources/On_Time_On_Time_Performance_2014_1.avro");
    QueryGenerator qg = new QueryGenerator(Collections.singletonList(avroFile), "whatever", "whatever");
    for (int i = 0; i < 100; i++) {
      System.out.println(qg.generateQuery().generatePql());
    }
  }
}
