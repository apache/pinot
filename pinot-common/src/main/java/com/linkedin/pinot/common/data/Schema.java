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
package com.linkedin.pinot.common.data;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.helix.ZNRecord;
import org.codehaus.jackson.annotate.JsonIgnore;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import static com.linkedin.pinot.common.utils.CommonConstants.Helix.*;
import com.linkedin.pinot.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  private static final Logger LOGGER = LoggerFactory.getLogger(Schema.class);
  private final Map<String, FieldSpec> fieldSpecMap = new HashMap<String, FieldSpec>();
  private String timeColumnName;
  private final List<String> dimensions;
  private final List<String> metrics;

  public Schema() {
    this.dimensions = new LinkedList<String>();
    this.metrics = new LinkedList<String>();
  }

  public void addSchema(String columnName, FieldSpec fieldSpec) {
    if (fieldSpec.getName() == null) {
      fieldSpec.setName(columnName);
    }

    if (fieldSpecMap.containsKey(columnName)) {
      return;
    }

    if (columnName != null) {
      fieldSpecMap.put(columnName, fieldSpec);

      if (fieldSpec.getFieldType() == FieldType.DIMENSION) {
        dimensions.add(columnName);
        Collections.sort(dimensions);
      } else if (fieldSpec.getFieldType() == FieldType.METRIC) {
        metrics.add(columnName);
        Collections.sort(metrics);
      } else if (fieldSpec.getFieldType() == FieldType.TIME) {
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

  @JsonIgnore
  public Collection<String> getColumnNames() {
    return fieldSpecMap.keySet();
  }

  public int size() {
    return fieldSpecMap.size();
  }

  public FieldSpec getFieldSpecFor(String column) {
    return fieldSpecMap.get(column);
  }

  @JsonIgnore
  public Collection<FieldSpec> getAllFieldSpecs() {
    return fieldSpecMap.values();
  }

  @JsonIgnore
  public List<String> getDimensionNames() {
    return dimensions;
  }

  @JsonIgnore
  public List<String> getMetricNames() {
    return metrics;
  }

  public String getTimeColumnName() {
    return timeColumnName;
  }

  @JsonIgnore
  public TimeFieldSpec getTimeSpec() {
    return (TimeFieldSpec) fieldSpecMap.get(timeColumnName);
  }

  // Added getters for Json annotator to work for args4j.
  public Map<String, FieldSpec> getFieldSpecMap() {
    return fieldSpecMap;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public void setTimeColumnName(String timeColumnName) {
    this.timeColumnName = timeColumnName;
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
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("Caught exception while processing field " + field, ex);
        }
      }
      result.append(newLine);
    }
    result.append("}");

    return result.toString();
  }

  public Map<String, String> toMap() {
    Map<String, String> schemaMap = new HashMap<String, String>();
    for (String fieldName : fieldSpecMap.keySet()) {
      FieldSpec fieldSpec = fieldSpecMap.get(fieldName);

      schemaMap.put(StringUtil.join(".", DataSource.SCHEMA, fieldName, DataSource.Schema.COLUMN_NAME),
          fieldSpec.getName());
      FieldType fieldType = fieldSpec.getFieldType();
      schemaMap.put(StringUtil.join(".", DataSource.SCHEMA, fieldName, DataSource.Schema.FIELD_TYPE),
          fieldType.toString());
      schemaMap.put(StringUtil.join(".", DataSource.SCHEMA, fieldName, DataSource.Schema.DATA_TYPE),
          fieldSpec.getDataType().toString());

      switch (fieldType) {
        case DIMENSION:
          schemaMap.put(StringUtil.join(".", DataSource.SCHEMA, fieldName, DataSource.Schema.IS_SINGLE_VALUE),
              fieldSpec.isSingleValueField() + "");
          schemaMap.put(StringUtil.join(".", DataSource.SCHEMA, fieldName, DataSource.Schema.DELIMETER),
              fieldSpec.getDelimiter());
          break;
        case TIME:
          schemaMap.put(StringUtil.join(".",
              DataSource.SCHEMA, fieldName, DataSource.Schema.TIME_UNIT),
              ((TimeFieldSpec) fieldSpec).getIncominGranularutySpec().getTimeType().toString());
          break;
        default:
          break;
      }
    }
    return schemaMap;
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
      spec.setDelimiter(delimiter);

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

  public static Schema getSchemaFromMap(Map<String, String> schemaConfig) {
    SchemaBuilder schemaBuilder = new SchemaBuilder();

    for (String configKey : schemaConfig.keySet()) {

      if (!configKey.startsWith(DataSource.SCHEMA) || !configKey.endsWith(DataSource.Schema.COLUMN_NAME)) {
        continue;
      }
      String columnName = schemaConfig.get(configKey);
      FieldType fieldType = FieldType.valueOf(schemaConfig.get(StringUtil.join(".", DataSource.SCHEMA, columnName, DataSource.Schema.FIELD_TYPE)).toUpperCase());
      DataType dataType = DataType.valueOf(schemaConfig.get(StringUtil.join(".", DataSource.SCHEMA, columnName, DataSource.Schema.DATA_TYPE)));

      switch (fieldType) {
        case DIMENSION:
          boolean isSingleValueField =
              Boolean.valueOf(schemaConfig.get(StringUtil.join(".", DataSource.SCHEMA, columnName, DataSource.Schema.IS_SINGLE_VALUE)));
          if (!isSingleValueField) {
            String delimeter = null;
            Object obj = schemaConfig.get(StringUtil.join(".", DataSource.SCHEMA, columnName, DataSource.Schema.DELIMETER));
            if (obj instanceof String) {
              delimeter = (String) obj;
            } else if (obj instanceof ArrayList) {
              delimeter = ",";
            }
            schemaBuilder.addMultiValueDimension(columnName, dataType, delimeter);
          } else {
            schemaBuilder.addSingleValueDimension(columnName, dataType);
          }
          break;
        case METRIC:
          schemaBuilder.addMetric(columnName, dataType);
          break;
        case TIME:
          TimeUnit timeUnit = TimeUnit.valueOf(schemaConfig.get(StringUtil.join(".", DataSource.SCHEMA, columnName, DataSource.Schema.TIME_UNIT)));
          schemaBuilder.addTime(columnName, timeUnit, dataType);
          break;
        default:
          throw new RuntimeException("Unable to recongize field type for column: " + columnName + ", fieldType = "
              + schemaConfig.get(StringUtil.join(".", DataSource.SCHEMA, columnName, DataSource.Schema.FIELD_TYPE)));
      }
    }
    return schemaBuilder.build();
  }

  public static Schema fromZNRecord(ZNRecord record) {
    return getSchemaFromMap(record.getSimpleFields());
  }

}
