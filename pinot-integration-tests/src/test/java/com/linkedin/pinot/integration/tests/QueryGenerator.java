package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.common.utils.StringUtil;
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
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;


/**
 * Utility class to generate random SQL queries based on an Avro file.
 *
 * @author jfim
 */
public class QueryGenerator {
  private Map<String, SortedSet<String>> _columnToValues = new HashMap<String, SortedSet<String>>();
  private Map<String, List<String>> _columnToValueList = new HashMap<String, List<String>>();
  private List<String> _columnNames = new ArrayList<String>();
  private List<String> _numericalColumnNames = new ArrayList<String>();
  private List<QueryGenerationStrategy> _queryGenerationStrategies = Arrays.<QueryGenerationStrategy>asList(
      new AggregationQueryGenerationStrategy()
  );
  private List<String> _booleanOperators = Arrays.asList("OR", "AND");
  private List<PredicateGenerator> _predicateGenerators = Arrays.asList(
      new ComparisonOperatorPredicateGenerator(),
      new InPredicateGenerator(),
      new BetweenPredicateGenerator()
  );
  private static final Random RANDOM = new Random();
  private final String _pqlTableName;
  private final String _h2TableName;

  public QueryGenerator(final File avroFile, final String pqlTableName, String h2TableName) {
    _pqlTableName = pqlTableName;
    _h2TableName = h2TableName;
    // Read schema and initialize storage
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> fileReader = null;
    try {
      fileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);

      Schema schema = fileReader.getSchema();
      for (Schema.Field field : schema.getFields()) {
        try {
          // Is this a union type?
          List<Schema> types = field.schema().getTypes();

          String name = field.name();
          _columnNames.add(name);
          _columnToValues.put(name, new TreeSet<String>());

          // We assume here that we can only have strings and numerical values, no arrays, unions, etc.
          if (types.get(0).getType() != Schema.Type.STRING) {
            _numericalColumnNames.add(name);
          }
        } catch (Exception e) {
          // Not a union type
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      IOUtils.closeQuietly(fileReader);
    }
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

          String value = null;
          Object avroValue = genericRecord.get(columnName);

          // Turn the value into a valid SQL token
          if (avroValue == null) {
            continue;
          } else if (avroValue instanceof Utf8) {
            value = "'" + avroValue.toString().replace("'", "''") + "'";
          } else {
            value = avroValue.toString();
          }

          values.add(value);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      IOUtils.closeQuietly(fileReader);
    }
  }

  /**
   * Finishes initialization of the query generator, once all Avro data has been loaded.
   */
  public void prepareToGenerateQueries() {
    for (String columnName : _columnNames) {
      _columnToValueList.put(columnName, new ArrayList<String>(_columnToValues.get(columnName)));
    }

    // Free the other copy of the data
    _columnToValues = null;
  }

  private interface QueryFragment {
    public String generatePql();
    public String generateH2Sql();
  }

  public interface Query {
    public String generatePql();
    public List<String> generateH2Sql();
  }

  private<T> T pickRandom(List<T> items) {
    return items.get(RANDOM.nextInt(items.size()));
  }

  public Query generateQuery() {
    return pickRandom(_queryGenerationStrategies).generateQuery();
  }

  private interface QueryGenerationStrategy {
    public Query generateQuery();
  }

  private class StringQueryFragment implements QueryFragment {
    private String querySql;

    private StringQueryFragment(String querySql) {
      this.querySql = querySql;
    }

    @Override
    public String generatePql() {
      return querySql;
    }

    @Override
    public String generateH2Sql() {
      return querySql;
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
      return generatePql();
    }
  }

  private QueryFragment generatePredicate() {
    int predicateCount = RANDOM.nextInt(10);

    List<QueryFragment> predicates = new ArrayList<QueryFragment>();
    for (int i = 0; i < predicateCount; i++) {
      String columnName = pickRandom(_columnNames);
      if (!_columnToValueList.get(columnName).isEmpty()) {
        predicates.add(pickRandom(_predicateGenerators).generatePredicate(columnName));
      }
    }

    if (predicates.size() < 2) {
      return new PredicateQueryFragment(predicates, Collections.<QueryFragment>emptyList());
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
/*
      // Generate a list of 1-10 columns
      List<String> columns =

      // Generate predicate
      String predicate = generatePredicate();

      // Generate limit statement
      String limit = "blah";

      return "SELECT " + projection + " FROM " + _tableName + " " + predicate + " " + limit;*/
      throw new RuntimeException("Unimplemented");
    }
  }

  private static String joinWithCommas(List<String>... elements) {
    List<String> joinedList = new ArrayList<String>();
    for (List<String> element : elements) {
      joinedList.addAll(element);
    }

    return StringUtil.join(", ", joinedList.toArray(new String[joinedList.size()]));
  }

  private class AggregationQuery implements Query {
    private List<String> _groupColumns;
    private List<String> _aggregateColumnsAndFunctions;
    private QueryFragment _predicate;

    public AggregationQuery(List<String> groupColumns, List<String> aggregateColumnsAndFunctions, QueryFragment predicate) {
      this._groupColumns = groupColumns;
      this._aggregateColumnsAndFunctions = aggregateColumnsAndFunctions;
      this._predicate = predicate;
    }

    @Override
    public String generatePql() {
      // Unlike SQL, PQL doesn't expect the group columns in select statements
      String queryBody = "SELECT " + joinWithCommas(_aggregateColumnsAndFunctions) + " FROM " + _pqlTableName + " " +
          _predicate.generatePql();

      if (_groupColumns.isEmpty()) {
        return queryBody;
      } else {
        return queryBody + " GROUP BY " + joinWithCommas(_groupColumns);
      }
    }

    @Override
    public List<String> generateH2Sql() {
      List<String> queries = new ArrayList<String>();
      if (_groupColumns.isEmpty()) {
        for (String aggregateColumnAndFunction : _aggregateColumnsAndFunctions) {
          queries.add("SELECT " + aggregateColumnAndFunction + " FROM " + _h2TableName + " " + _predicate.generatePql());
        }
      } else {
        for (String aggregateColumnAndFunction : _aggregateColumnsAndFunctions) {
          queries.add("SELECT " + joinWithCommas(_groupColumns) + ", " + aggregateColumnAndFunction +
              " FROM " + _h2TableName + " " + _predicate.generatePql() + " GROUP BY " + joinWithCommas(_groupColumns));
        }
      }
      return queries;
    }
  }

  /**
   * Queries similar to SELECT foo, SUM(bar) FROM blah WHERE ... GROUP BY foo
   */
  private class AggregationQueryGenerationStrategy implements QueryGenerationStrategy {
    private final List<String> aggregationFunctions = Arrays.asList("SUM", "MIN", "MAX", "COUNT", "AVG");
    @Override
    public Query generateQuery() {
      // Generate 0-9 columns on which to group
      Set<String> groupColumns = new HashSet<String>();
      int groupColumnCount = RANDOM.nextInt(10);
      for (int i = 0; i < groupColumnCount; i++) {
        groupColumns.add(pickRandom(_columnNames));
      }

      // Generate a disjoint set of 0-9 columns on which to aggregate
      int aggregationColumnCount = RANDOM.nextInt(10);
      Set<String> aggregationColumns = new HashSet<String>();
      for (int i = 0; i < aggregationColumnCount; i++) {
        String randomColumn = pickRandom(_numericalColumnNames);
        if (!groupColumns.contains(randomColumn))
          aggregationColumns.add(randomColumn);
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

      // Generate a predicate
      QueryFragment predicate = generatePredicate();

      return new AggregationQuery(new ArrayList<String>(groupColumns), aggregationColumnsAndFunctions, predicate);
    }
  }

  private interface PredicateGenerator {
    public QueryFragment generatePredicate(String columnName);
  }

  private class ComparisonOperatorPredicateGenerator implements PredicateGenerator {
    private List<String> _comparisonOperators = Arrays.asList("=", "<>", "<", ">", "<=", ">=");
    @Override
    public QueryFragment generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);
      return new StringQueryFragment(columnName + " " + pickRandom(_comparisonOperators) + " " + pickRandom(columnValues));
    }
  }

  private class InPredicateGenerator implements PredicateGenerator {
    @Override
    public QueryFragment generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);

      int inValueCount = RANDOM.nextInt(100);
      List<String> inValueList = new ArrayList<String>(inValueCount);
      for (int i = 0; i < inValueCount; i++) {
        inValueList.add(pickRandom(columnValues));
      }
      String inValues = StringUtil.join(", ", inValueList.toArray(new String[inValueList.size()]));

      return new StringQueryFragment(columnName + " IN (" + inValues + ")");
    }
  }

  private class BetweenPredicateGenerator implements PredicateGenerator {
    @Override
    public QueryFragment generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);
      return new StringQueryFragment(columnName + " BETWEEN " + pickRandom(columnValues) + " AND " + pickRandom(columnValues));
    }
  }

  public static void main(String[] args) {
    File avroFile = new File("pinot-integration-tests/src/test/resources/On_Time_On_Time_Performance_2014_1.avro");
    QueryGenerator qg = new QueryGenerator(avroFile, "whatever", "whatever");
    qg.addAvroData(avroFile);
    qg.prepareToGenerateQueries();
    for (int i = 0; i < 100; i++) {
      System.out.println(qg.generateQuery().generatePql());
    }
  }
}
