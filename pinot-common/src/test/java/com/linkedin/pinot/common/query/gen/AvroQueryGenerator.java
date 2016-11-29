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
package com.linkedin.pinot.common.query.gen;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;

import com.linkedin.pinot.common.data.FieldSpec.DataType;

public class AvroQueryGenerator {

  public interface TestAggreationQuery {
    String getPql();
  }

  public static class TestSimpleAggreationQuery implements TestAggreationQuery {
    public final String pql;
    public final Double result;

    public TestSimpleAggreationQuery(String pql, Double result) {
      this.pql = pql;
      this.result = result;
    }

    @Override
    public String toString() {
      return pql + " : " + result;
    }

    @Override
    public String getPql() {
      return pql;
    }
  }

  public static class TestGroupByAggreationQuery implements TestAggreationQuery {
    public final String pql;
    public final Map<Object, Double> groupResults;

    public TestGroupByAggreationQuery(String pql, Map<Object, Double> groupResults) {
      this.pql = pql;
      this.groupResults = groupResults;
    }

    @Override
    public String toString() {
      final StringBuilder bld = new StringBuilder();
      bld.append(pql + " : ");
      for (final Object key : groupResults.keySet()) {
        bld.append(key + ":" + groupResults.get(key) + ",");
      }
      return bld.toString();
    }

    @Override
    public String getPql() {
      return pql;
    }
  }

  private final File avroFile;
  private final List<String> dimensions;
  private final List<String> metrics;
  private final String time;
  private final Map<String, DataType> dataTypeMap;
  private final Map<String, Boolean> isSingleValueMap;

  private DataFileStream<GenericRecord> dataStream;
  private Schema schema;
  private List<TestSimpleAggreationQuery> aggregationQueries;
  private List<TestGroupByAggreationQuery> groupByQueries;
  private final String resourceName;
  private boolean isRealtimeSegment = false;

  public AvroQueryGenerator(File avroFile, List<String> dimensions, List<String> metrics, String time,
      String resourceName) throws FileNotFoundException, IOException {
    this.avroFile = avroFile;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.time = time;
    dataTypeMap = new HashMap<String, DataType>();
    isSingleValueMap = new HashMap<String, Boolean>();
    this.resourceName = resourceName;
  }

  public AvroQueryGenerator(File avroFile, List<String> dimensions, List<String> metrics, String time,
      String resourceName, boolean isRealtimeSegment) throws FileNotFoundException, IOException {
    this.avroFile = avroFile;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.time = time;
    dataTypeMap = new HashMap<String, DataType>();
    isSingleValueMap = new HashMap<String, Boolean>();
    this.resourceName = resourceName;
    this.isRealtimeSegment = isRealtimeSegment;
  }

  public void init() throws FileNotFoundException, IOException {
    dataStream =
        new DataFileStream<GenericRecord>(new FileInputStream(avroFile), new GenericDatumReader<GenericRecord>());
    schema = dataStream.getSchema();
  }

  public List<TestSimpleAggreationQuery> giveMeNSimpleAggregationQueries(int n) {
    Collections.shuffle(aggregationQueries, new Random(System.currentTimeMillis()));
    if (n <= aggregationQueries.size()) {
      return aggregationQueries.subList(0, n);
    }
    return aggregationQueries;
  }

  public List<TestGroupByAggreationQuery> giveMeNGroupByAggregationQueries(int n) {
    Collections.shuffle(groupByQueries, new Random(System.currentTimeMillis()));
    if (n <= aggregationQueries.size()) {
      return groupByQueries.subList(0, n);
    }
    return groupByQueries;
  }

  public void temp() {

  }

  public void generateSimpleAggregationOnSingleColumnFilters() throws IOException {
    final Map<String, Map<Object, Integer>> cardinalityCountsMap = new HashMap<String, Map<Object, Integer>>();
    final Map<String, Map<Object, Map<String, Double>>> sumMap =
        new HashMap<String, Map<Object, Map<String, Double>>>();
    // here string key is columnName:columnValue:MetricName:GroupColumnName:groupKey:metricValue

    final Map<String, Map<Object, Double>> sumGroupBy = new HashMap<String, Map<Object, Double>>();

    aggregationQueries = new ArrayList<AvroQueryGenerator.TestSimpleAggreationQuery>();
    groupByQueries = new ArrayList<AvroQueryGenerator.TestGroupByAggreationQuery>();
    for (final Field f : schema.getFields()) {
      final String fieldName = f.name();
      if (dimensions.contains(fieldName) || metrics.contains(fieldName) || time.equals(fieldName)) {
        isSingleValueMap.put(fieldName, isSingleValueField(f));
        dataTypeMap.put(fieldName, getColumnType(f));
        if (!metrics.contains(fieldName)) {
          cardinalityCountsMap.put(fieldName, new HashMap<Object, Integer>());
        }
      }
    }

    for (final String column : cardinalityCountsMap.keySet()) {
      sumMap.put(column, new HashMap<Object, Map<String, Double>>());
    }

    // here string key is columnName:columnValue:MetricName:GroupColumnName:groupKey:metricValue

    while (dataStream.hasNext()) {
      final GenericRecord record = dataStream.next();

      for (final String column : cardinalityCountsMap.keySet()) {
        Object value = record.get(column);

        if (value == null) {
          switch (schema.getField(column).schema().getType()) {
            case INT:
              value = 0;
              break;
            case FLOAT:
              value = 0F;
              break;
            case LONG:
              value = 0L;
              break;
            case DOUBLE:
              value = 0D;
              break;
            case STRING:
            case BOOLEAN:
              value = "null";
              break;
          }
        }

        if (value instanceof Utf8) {
          value = ((Utf8) value).toString();
        }

        if (value instanceof Array) {
          continue;
        }

        // here string key is columnName:columnValue:MetricName:GroupColumnName:groupKey:metricValue

        for (final String metricName : metrics) {
          final String groupbyKeyBase = column + ":" + record.get(column) + ":" + metricName;
          int dimCounter = 1;
          for (final String dim : cardinalityCountsMap.keySet()) {
            if (!dim.equals(column)) {
              dimCounter++;
              final String groupbyKey = groupbyKeyBase + ":" + dim;
              if (sumGroupBy.containsKey(groupbyKey)) {
                if (sumGroupBy.get(groupbyKey).containsKey(record.get(dim))) {
                  sumGroupBy.get(groupbyKey).put(
                      record.get(dim),
                      getAppropriateNumberType(metricName, record.get(metricName),
                          sumGroupBy.get(groupbyKey).get(record.get(dim))));
                } else {
                  sumGroupBy.get(groupbyKey)
                      .put(record.get(dim), Double.parseDouble(record.get(metricName).toString()));
                }
              } else {
                sumGroupBy.put(groupbyKey, new HashMap<Object, Double>());
                sumGroupBy.get(groupbyKey).put(record.get(dim), Double.parseDouble(record.get(metricName).toString()));
              }
            }
            if (dimCounter == 4) {
              break;
            }
          }
        }

        if (cardinalityCountsMap.get(column).containsKey(value)) {
          cardinalityCountsMap.get(column).put(value, cardinalityCountsMap.get(column).get(value) + 1);
        } else {
          cardinalityCountsMap.get(column).put(value, 1);
        }

        if (!sumMap.get(column).containsKey(value)) {
          sumMap.get(column).put(value, new HashMap<String, Double>());
        }

        for (final String metric : metrics) {
          if (!sumMap.get(column).get(value).containsKey(metric)) {
            sumMap.get(column).get(value).put(metric, getAppropriateNumberType(metric, record.get(metric), 0D));
          } else {
            sumMap
                .get(column)
                .get(value)
                .put(metric,
                    getAppropriateNumberType(metric, record.get(metric), sumMap.get(column).get(value).get(metric)));
          }
        }
        // here string key is columnName:columnValue:MetricName:GroupColumnName:groupKey:metricValue
      }
    }

    dataStream.close();

    if (!isRealtimeSegment) {
      for (final String column : cardinalityCountsMap.keySet()) {
        for (final Object entry : cardinalityCountsMap.get(column).keySet()) {
          final StringBuilder bld = new StringBuilder();
          bld.append("select count(*) from ");
          bld.append(resourceName);
          bld.append(" where ");
          bld.append(column);
          bld.append("=");
          bld.append("'");
          bld.append(entry);
          bld.append("'");
          bld.append(" ");
          bld.append("limit 0");
          String queryString = bld.toString();
          if (!queryString.contains("null")) {
            aggregationQueries.add(new TestSimpleAggreationQuery(queryString, new Double(cardinalityCountsMap.get(
                column).get(entry))));
          }
        }
      }
    }

    for (final String column : sumMap.keySet()) {
      for (final Object value : sumMap.get(column).keySet()) {
        for (final String metric : sumMap.get(column).get(value).keySet()) {
          final StringBuilder bld = new StringBuilder();
          bld.append("select sum('" + metric + "') from ");
          bld.append(resourceName);
          bld.append(" where ");
          bld.append(column);
          bld.append("=");
          bld.append("'");
          bld.append(value);
          bld.append("'");
          bld.append(" ");
          bld.append("limit 0");
          String queryString = bld.toString();
          if (!queryString.contains("null")) {
            aggregationQueries.add(new TestSimpleAggreationQuery(bld.toString(), sumMap.get(column).get(value)
                .get(metric)));
          }
        }
      }
    }

    for (final String groupKey : sumGroupBy.keySet()) {
      final String columnName = groupKey.split(":")[0];
      final String columnValue = groupKey.split(":")[1];
      final String metricColumn = groupKey.split(":")[2];
      final String groupByColumnName = groupKey.split(":")[3];

      final StringBuilder bld = new StringBuilder();
      bld.append("select sum('" + metricColumn + "') from ");
      bld.append(resourceName);
      bld.append(" where ");
      bld.append(columnName);
      bld.append("=");
      bld.append("'");
      bld.append(columnValue);
      bld.append("'");
      bld.append(" ");
      bld.append(" group by ");
      bld.append(groupByColumnName);
      bld.append(" top 10 ");
      bld.append("limit 0");
      String queryString = bld.toString();
      if (!queryString.contains("null")) {
        groupByQueries.add(new TestGroupByAggreationQuery(bld.toString(), sumGroupBy.get(groupKey)));
      }
    }
  }

  private Double getAppropriateNumberType(String column, Object entry, Double sum) {
    switch (dataTypeMap.get(column)) {
      case INT:
        return sum + (Integer) entry;
      case FLOAT:
        return sum + (Float) entry;
      case LONG:
        return sum + (Long) entry;
      case DOUBLE:
        return sum + (Double) entry;
      default:
        return 0D;
    }
  }

  private static boolean isSingleValueField(Field field) {
    org.apache.avro.Schema fieldSchema = field.schema();
    fieldSchema = extractSchemaFromUnionIfNeeded(fieldSchema);

    final Type type = fieldSchema.getType();
    if (type == Type.ARRAY) {
      return false;
    }
    return true;
  }

  public static DataType getColumnType(Field field) {
    org.apache.avro.Schema fieldSchema = field.schema();
    fieldSchema = extractSchemaFromUnionIfNeeded(fieldSchema);

    final Type type = fieldSchema.getType();
    if (type == Type.ARRAY) {
      org.apache.avro.Schema elementSchema = extractSchemaFromUnionIfNeeded(fieldSchema.getElementType());
      if (elementSchema.getType() == Type.RECORD) {
        if (elementSchema.getFields().size() == 1) {
          elementSchema = elementSchema.getFields().get(0).schema();
        } else {
          throw new RuntimeException("More than one schema in Multi-value column!");
        }
        elementSchema = extractSchemaFromUnionIfNeeded(elementSchema);
      }
      return DataType.valueOf(elementSchema.getType());
    } else {
      return DataType.valueOf(type);
    }
  }

  private static org.apache.avro.Schema extractSchemaFromUnionIfNeeded(org.apache.avro.Schema fieldSchema) {
    if ((fieldSchema).getType() == Type.UNION) {
      fieldSchema = ((org.apache.avro.Schema) CollectionUtils.find(fieldSchema.getTypes(), new Predicate() {
        @Override
        public boolean evaluate(Object object) {
          return ((org.apache.avro.Schema) object).getType() != Type.NULL;
        }
      }));
    }
    return fieldSchema;
  }

  private static Object[] transformAvroArrayToObjectArray(Array arr) {
    if (arr == null) {
      return new Object[0];
    }
    final Object[] ret = new Object[arr.size()];
    final Iterator iterator = arr.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      Object value = iterator.next();
      if (value instanceof Record) {
        value = ((Record) value).get(0);
      }
      if (value instanceof Utf8) {
        value = ((Utf8) value).toString();
      }
      ret[i++] = value;
    }
    return ret;
  }

}
