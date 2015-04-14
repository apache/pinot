package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.common.utils.StringUtil;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
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
  private final String _tableName;

  public QueryGenerator(final File avroFile, final String tableName) {
    _tableName = tableName;
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
  }

  private<T> T pickRandom(List<T> items) {
    return items.get(RANDOM.nextInt(items.size()));
  }

  public String generateQuery() {
    return pickRandom(_queryGenerationStrategies).generateQuery();
  }

  private interface QueryGenerationStrategy {
    public String generateQuery();
  }

  private String generatePredicate() {
    int predicateCount = RANDOM.nextInt(10);

    List<String> predicates = new ArrayList<String>();
    for (int i = 0; i < predicateCount; i++) {
      String columnName = pickRandom(_columnNames);
      if (!_columnToValues.get(columnName).isEmpty()) {
        predicates.add(pickRandom(_predicateGenerators).generatePredicate(columnName));
      }
    }

    if (predicates.isEmpty()) {
      return "";
    }

    // Join predicates with ANDs and ORs
    String predicateString = "WHERE " + predicates.get(0);
    for (int i = 1; i < predicates.size(); i++) {
      String predicate = predicates.get(i);
      predicateString = predicateString + " " + pickRandom(_booleanOperators) + " " + predicate;
    }

    return predicateString;
  }

  /**
   * Queries similar to SELECT blah FROM blah WHERE ... LIMIT blah
   */
  private class SelectionQueryGenerationStrategy implements QueryGenerationStrategy {
    @Override
    public String generateQuery() {
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

  /**
   * Queries similar to SELECT foo, SUM(bar) FROM blah WHERE ... GROUP BY foo
   */
  private class AggregationQueryGenerationStrategy implements QueryGenerationStrategy {
    private final List<String> aggregationFunctions = Arrays.asList("SUM", "MIN", "MAX", "COUNT", "AVG");
    @Override
    public String generateQuery() {
      // Generate 0-9 columns on which to group
      Set<String> groupColumns = new HashSet<String>();
      int groupColumnCount = RANDOM.nextInt(10);
      for (int i = 0; i < groupColumnCount; i++) {
        groupColumns.add(pickRandom(_columnNames));
      }

      String groupByColumns = "";
      for (String groupColumn : groupColumns) {
        groupByColumns += groupColumn + ", ";
      }

      // Create a group by clause
      String groupByClause;
      if (groupColumns.isEmpty()) {
        groupByClause = "";
      } else {
        groupByClause = "GROUP BY " + StringUtil.join(", ", groupColumns.toArray(new String[groupColumns.size()]));
      }

      // Generate a disjoint set of 0-9 columns on which to aggregate
      String aggregations;
      int aggregationColumnCount = RANDOM.nextInt(10);
      Set<String> aggregationColumns = new HashSet<String>();
      for (int i = 0; i < aggregationColumnCount; i++) {
        String randomColumn = pickRandom(_numericalColumnNames);
        if (!groupColumns.contains(randomColumn))
          aggregationColumns.add(randomColumn);
      }
      if (aggregationColumns.isEmpty()) {
        aggregations = "COUNT(*)";
      } else {
        List<String> aggregationElements = new ArrayList<String>();
        for (String aggregationColumn : aggregationColumns) {
          int aggregationFunctionCount = RANDOM.nextInt(aggregationFunctions.size()) + 1;
          for (int i = 0; i < aggregationFunctionCount; i++) {
            aggregationElements.add(pickRandom(aggregationFunctions) + "(" + aggregationColumn + ")");
          }
        }
        if (aggregationElements.size() == 1) {
          aggregations = aggregationElements.get(0);
        } else {
          aggregations = StringUtil.join(", ", aggregationElements.toArray(new String[aggregationElements.size()]));
        }
      }

      // Generate a predicate
      String predicate = generatePredicate();

      if (groupByColumns.isEmpty() && aggregations.isEmpty()) {
        return "SELECT COUNT(*) FROM " + _tableName + " " + predicate;
      } else {
        return "SELECT " + groupByColumns + aggregations + " FROM " + _tableName + " " + predicate + " " + groupByClause;
      }
    }
  }

  private interface PredicateGenerator {
    public String generatePredicate(String columnName);
  }

  private class ComparisonOperatorPredicateGenerator implements PredicateGenerator {
    private List<String> _comparisonOperators = Arrays.asList("=", "<>", "<", ">", "<=", ">=");
    @Override
    public String generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);
      return columnName + " " + pickRandom(_comparisonOperators) + " " + pickRandom(columnValues);
    }
  }

  private class InPredicateGenerator implements PredicateGenerator {
    @Override
    public String generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);

      int inValueCount = RANDOM.nextInt(100);
      List<String> inValueList = new ArrayList<String>(inValueCount);
      for (int i = 0; i < inValueCount; i++) {
        inValueList.add(pickRandom(columnValues));
      }
      String inValues = StringUtil.join(", ", inValueList.toArray(new String[inValueList.size()]));

      return columnName + " IN (" + inValues + ")";
    }
  }

  private class BetweenPredicateGenerator implements PredicateGenerator {
    @Override
    public String generatePredicate(String columnName) {
      List<String> columnValues = _columnToValueList.get(columnName);
      return columnName + " BETWEEN " + pickRandom(columnValues) + " AND " + pickRandom(columnValues);
    }
  }

  public static void main(String[] args) {
    File avroFile = new File("pinot-integration-tests/src/test/resources/On_Time_On_Time_Performance_2014_1.avro");
    QueryGenerator qg = new QueryGenerator(avroFile, "whatever");
    qg.addAvroData(avroFile);
    qg.prepareToGenerateQueries();
    for (int i = 0; i < 100; i++) {
      System.out.println(qg.generateQuery());
    }
  }
}
