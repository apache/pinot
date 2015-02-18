package com.linkedin.pinot.common.data;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;


/**
 * Schema is defined for each column. To describe the details information of columns.
 * Three types of information are provided.
 * 1. the data type of this column: int, long, double...
 * 2. if this column is a single value column or a multi-value column.
 * 3. the real world business logic: dimensions, metrics and timeStamps.
 * Different indexing and query strategies are used for different data schema types.
 *
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public class Schema {
  private final Map<String, FieldSpec> fieldSpecMap = new HashMap<String, FieldSpec>();
  private String timeColumnName;
  private final List<String> dimensions;
  private final List<String> metrics;

  public Schema() {
    this.dimensions = new LinkedList<String>();
    this.metrics = new LinkedList<String>();
  }

  public void addSchema(String columnName, FieldSpec fieldSpec) {
    if (fieldSpec.getName() == null)
      fieldSpec.setName(columnName);

    if (fieldSpecMap.containsKey(columnName))
      return;

    if (columnName != null && fieldSpec != null) {
      fieldSpecMap.put(columnName, fieldSpec);
      if (fieldSpec.getFieldType() == FieldType.dimension) {
        dimensions.add(columnName);
        Collections.sort(dimensions);
      } else if (fieldSpec.getFieldType() == FieldType.metric) {
        metrics.add(columnName);
        Collections.sort(metrics);
      } else if (fieldSpec.getFieldType() == FieldType.time) {
        timeColumnName = columnName;
      }
    }
  }

  public void removeSchema(String columnName) {
    if (fieldSpecMap.containsKey(columnName)) {
      fieldSpecMap.remove(columnName);
    }
  }

  public boolean isExisted(String columnName) {
    return fieldSpecMap.containsKey(columnName);
  }

  public Collection<String> getColumnNames() {
    return fieldSpecMap.keySet();
  }

  public int size() {
    return fieldSpecMap.size();
  }

  public FieldSpec getFieldSpecFor(String column) {
    return fieldSpecMap.get(column);
  }

  public Collection<FieldSpec> getAllFieldSpecs() {
    return fieldSpecMap.values();
  }

  public List<String> getDimensionNames() {
    return dimensions;
  }

  public List<String> getMetricNames() {
    return metrics;
  }

  public String getTimeColumnName() {
    return timeColumnName;
  }

  public TimeFieldSpec getTimeSpec() {
    return (TimeFieldSpec) fieldSpecMap.get(timeColumnName);
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    final String newLine = System.getProperty("line.separator");

    result.append(this.getClass().getName());
    result.append(" Object {");
    result.append(newLine);

    //determine fields declared in this class only (no fields of superclass)
    final Field[] fields = this.getClass().getDeclaredFields();

    //print field names paired with their values
    for (final Field field : fields) {
      result.append("  ");
      try {
        result.append(field.getName());
        result.append(": ");
        //requires access to private field:
        result.append(field.get(this));
      } catch (final IllegalAccessException ex) {
        System.out.println(ex);
      }
      result.append(newLine);
    }
    result.append("}");

    return result.toString();
  }

  public static class SchemaBuilder {
    private Schema schema;

    public SchemaBuilder() {
      schema = new Schema();
    }

    public SchemaBuilder addSingleValueDimension(String dimensionName, DataType type) {
      FieldSpec spec = new DimensionFieldSpec();
      spec.setSingleValueField(true);
      spec.setDataType(type);
      spec.setName(dimensionName);
      schema.addSchema(dimensionName, spec);
      return this;
    }

    public SchemaBuilder addMultiValueDimension(String dimensionName, DataType dataType, String delimiter) {
      FieldSpec spec = new DimensionFieldSpec();
      spec.setSingleValueField(false);
      spec.setDataType(dataType);
      spec.setName(dimensionName);
      spec.setDelimeter(delimiter);

      schema.addSchema(dimensionName, spec);
      return this;
    }

    public SchemaBuilder addMetric(String metricName, DataType dataType) {
      FieldSpec spec = new MetricFieldSpec();
      spec.setSingleValueField(true);
      spec.setDataType(dataType);
      spec.setName(metricName);

      schema.addSchema(metricName, spec);
      return this;
    }

    public SchemaBuilder addTime(String incomingColumnName, TimeUnit incomingGranularity, DataType incomingDataType) {
      TimeGranularitySpec incomingGranularitySpec =
          new TimeGranularitySpec(incomingDataType, incomingGranularity, incomingColumnName);

      schema.addSchema(incomingColumnName, new TimeFieldSpec(incomingGranularitySpec));
      return this;
    }

    public SchemaBuilder addTime(String incomingColumnName, TimeUnit incomingGranularity, DataType incomingDataType,
        String outGoingColumnName, TimeUnit outgoingGranularity, DataType outgoingDataType) {

      TimeGranularitySpec incoming = new TimeGranularitySpec(incomingDataType, incomingGranularity, incomingColumnName);
      TimeGranularitySpec outgoing = new TimeGranularitySpec(outgoingDataType, outgoingGranularity, outGoingColumnName);
      schema.addSchema(incomingColumnName, new TimeFieldSpec(incoming, outgoing));
      return this;
    }

    public Schema build() {
      return schema;
    }
  }
}
