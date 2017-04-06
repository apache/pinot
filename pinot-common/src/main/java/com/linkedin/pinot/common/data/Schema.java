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
package com.linkedin.pinot.common.data;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.utils.EqualityUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The <code>Schema</code> class is defined for each table to describe the details of the table's fields (columns).
 * <p>Three field types are supported: DIMENSION, METRIC, TIME.
 * ({@link com.linkedin.pinot.common.data.DimensionFieldSpec}, {@link com.linkedin.pinot.common.data.MetricFieldSpec},
 * {@link com.linkedin.pinot.common.data.TimeFieldSpec})
 * <p>For each field, a {@link com.linkedin.pinot.common.data.FieldSpec} is defined to provide the details of the field.
 * <p>There could be multiple DIMENSION or METRIC fields, but at most 1 TIME field.
 * <p>In pinot, we store data using 5 <code>DataType</code>s: INT, LONG, FLOAT, DOUBLE, STRING. All other
 * <code>DataType</code>s will be converted to one of them.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class Schema {
  private static final Logger LOGGER = LoggerFactory.getLogger(Schema.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String schemaName;
  private final List<DimensionFieldSpec> dimensionFieldSpecs = new ArrayList<>();
  private final List<MetricFieldSpec> metricFieldSpecs = new ArrayList<>();
  private TimeFieldSpec timeFieldSpec;
  private final Map<String, FieldSpec> fieldSpecMap = new HashMap<>();
  private final Set<String> dimensions = new HashSet<>();
  private final Set<String> metrics = new HashSet<>();
  private transient String jsonSchema;

  public static Schema fromFile(@Nonnull File schemaFile)
      throws IOException {
    return MAPPER.readValue(schemaFile, Schema.class);
  }

  public static Schema fromString(@Nonnull String schemaString)
      throws IOException {
    return MAPPER.readValue(schemaString, Schema.class);
  }

  public static Schema fromInputSteam(@Nonnull InputStream schemaInputStream)
      throws IOException {
    return MAPPER.readValue(schemaInputStream, Schema.class);
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(@Nonnull String schemaName) {
    Preconditions.checkNotNull(schemaName);

    this.schemaName = schemaName;
  }

  public List<DimensionFieldSpec> getDimensionFieldSpecs() {
    return dimensionFieldSpecs;
  }

  public void setDimensionFieldSpecs(@Nonnull List<DimensionFieldSpec> dimensionFieldSpecs) {
    Preconditions.checkState(this.dimensionFieldSpecs.isEmpty());

    for (DimensionFieldSpec dimensionFieldSpec : dimensionFieldSpecs) {
      addField(dimensionFieldSpec);
    }
  }

  public List<MetricFieldSpec> getMetricFieldSpecs() {
    return metricFieldSpecs;
  }

  public void setMetricFieldSpecs(@Nonnull List<MetricFieldSpec> metricFieldSpecs) {
    Preconditions.checkState(this.metricFieldSpecs.isEmpty());

    for (MetricFieldSpec metricFieldSpec : metricFieldSpecs) {
      addField(metricFieldSpec);
    }
  }

  public @Nullable TimeFieldSpec getTimeFieldSpec() {
    return timeFieldSpec;
  }

  public void setTimeFieldSpec(@Nonnull TimeFieldSpec timeFieldSpec) {
    // This check is for JSON de-serializer. For normal use case, should not set null.
    if (timeFieldSpec != null) {
      addField(timeFieldSpec);
    }
  }

  public void addField(@Nonnull FieldSpec fieldSpec) {
    Preconditions.checkNotNull(fieldSpec);
    String columnName = fieldSpec.getName();
    Preconditions.checkNotNull(columnName);
    Preconditions.checkState(!fieldSpecMap.containsKey(columnName),
        "Field spec already exists for column: " + columnName);

    FieldType fieldType = fieldSpec.getFieldType();
    switch (fieldType) {
      case DIMENSION:
        dimensions.add(columnName);
        dimensionFieldSpecs.add((DimensionFieldSpec) fieldSpec);
        break;
      case METRIC:
        metrics.add(columnName);
        metricFieldSpecs.add((MetricFieldSpec) fieldSpec);
        break;
      case TIME:
        Preconditions.checkState(timeFieldSpec == null, "Already defined the time column: " + timeFieldSpec);
        timeFieldSpec = (TimeFieldSpec) fieldSpec;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
    }

    fieldSpecMap.put(columnName, fieldSpec);
  }

  @Deprecated
  // For third-eye backward compatible.
  public void addField(@Nonnull String columnName, @Nonnull FieldSpec fieldSpec) {
    addField(fieldSpec);
  }

  public boolean hasColumn(@Nonnull String columnName) {
    return fieldSpecMap.containsKey(columnName);
  }

  @JsonIgnore
  public Map<String, FieldSpec> getFieldSpecMap() {
    return fieldSpecMap;
  }

  @JsonIgnore
  public Collection<String> getColumnNames() {
    return fieldSpecMap.keySet();
  }

  @JsonIgnore
  public Collection<FieldSpec> getAllFieldSpecs() {
    return fieldSpecMap.values();
  }

  public int size() {
    return fieldSpecMap.size();
  }

  @JsonIgnore
  public @Nullable FieldSpec getFieldSpecFor(String columnName) {
    return fieldSpecMap.get(columnName);
  }

  @JsonIgnore
  public @Nullable MetricFieldSpec getMetricSpec(String metricName) {
    FieldSpec fieldSpec = fieldSpecMap.get(metricName);
    if (fieldSpec != null && fieldSpec.getFieldType() == FieldType.METRIC) {
      return (MetricFieldSpec) fieldSpec;
    }
    return null;
  }

  @JsonIgnore
  public @Nullable DimensionFieldSpec getDimensionSpec(String dimensionName) {
    FieldSpec fieldSpec = fieldSpecMap.get(dimensionName);
    if (fieldSpec != null && fieldSpec.getFieldType() == FieldType.DIMENSION) {
      return (DimensionFieldSpec) fieldSpec;
    }
    return null;
  }

  @JsonIgnore
  public List<String> getDimensionNames() {
    return new ArrayList<>(dimensions);
  }

  @JsonIgnore
  public List<String> getMetricNames() {
    return new ArrayList<>(metrics);
  }

  @JsonIgnore
  public @Nullable String getTimeColumnName() {
    return (timeFieldSpec != null) ? timeFieldSpec.getName() : null;
  }

  @JsonIgnore
  public @Nullable TimeUnit getIncomingTimeUnit() {
    return (timeFieldSpec != null) ? timeFieldSpec.getIncomingGranularitySpec().getTimeType() : null;
  }

  @JsonIgnore
  public @Nullable TimeUnit getOutgoingTimeUnit() {
    return (timeFieldSpec != null) ? timeFieldSpec.getOutgoingGranularitySpec().getTimeType() : null;
  }

  @JsonIgnore
  public String getJSONSchema() {
    if (jsonSchema == null) {
      try {
        jsonSchema = MAPPER.writeValueAsString(this);
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while writing Schema as JSON format string.", e);
      }
    }
    return jsonSchema;
  }

  /**
   * Validates a pinot schema. The following validations are performed:
   * <p>- For dimension and time fields, support {@link DataType}: INT, LONG, FLOAT, DOUBLE, STRING.
   * <p>- For metric fields (non-derived), support {@link DataType}: INT, LONG, FLOAT, DOUBLE.
   * <p>- All fields must have a default null value.
   *
   * @param ctxLogger logger used to log the message (if null, the current class logger is used).
   * @return whether schema is valid.
   */
  public boolean validate(Logger ctxLogger) {
    if (ctxLogger == null) {
      ctxLogger = LOGGER;
    }
    boolean isValid = true;

    // Log ALL the schema errors that may be present.
    for (FieldSpec fieldSpec : fieldSpecMap.values()) {
      FieldType fieldType = fieldSpec.getFieldType();
      DataType dataType = fieldSpec.getDataType();
      String fieldName = fieldSpec.getName();
      try {
        switch (fieldType) {
          case DIMENSION:
          case TIME:
            switch (dataType) {
              case INT:
              case LONG:
              case FLOAT:
              case DOUBLE:
              case STRING:
                // Check getDefaultNullValue() does not throw exception.
                fieldSpec.getDefaultNullValue();
                break;
              default:
                ctxLogger.info("Unsupported data type: {} in DIMENSION/TIME field: {}", dataType, fieldName);
                isValid = false;
                break;
            }
            break;
          case METRIC:
            switch (dataType) {
              case INT:
              case LONG:
              case FLOAT:
              case DOUBLE:
                // Check getDefaultNullValue() does not throw exception.
                fieldSpec.getDefaultNullValue();
                break;
              default:
                ctxLogger.info("Unsupported data type: {} in METRIC field: {}", dataType, fieldName);
                isValid = false;
                break;
            }
            break;
          default:
            ctxLogger.info("Unsupported field type: {} for field: {}", dataType, fieldName);
            isValid = false;
            break;
        }
      } catch (Exception e) {
        ctxLogger.info("Caught exception while validating field: {} with field type: {}, data type: {}, {}", fieldName,
            fieldType, dataType, e.getMessage());
        isValid = false;
      }
    }

    return isValid;
  }

  public static class SchemaBuilder {
    private Schema schema;

    public SchemaBuilder() {
      schema = new Schema();
    }

    public SchemaBuilder setSchemaName(@Nonnull String schemaName) {
      schema.setSchemaName(schemaName);
      return this;
    }

    public SchemaBuilder addSingleValueDimension(@Nonnull String dimensionName, @Nonnull DataType dataType) {
      schema.addField(new DimensionFieldSpec(dimensionName, dataType, true));
      return this;
    }

    public SchemaBuilder addSingleValueDimension(@Nonnull String dimensionName, @Nonnull DataType dataType,
        @Nonnull Object defaultNullValue) {
      schema.addField(new DimensionFieldSpec(dimensionName, dataType, true, defaultNullValue));
      return this;
    }

    public SchemaBuilder addMultiValueDimension(@Nonnull String dimensionName, @Nonnull DataType dataType) {
      schema.addField(new DimensionFieldSpec(dimensionName, dataType, false));
      return this;
    }

    public SchemaBuilder addMultiValueDimension(@Nonnull String dimensionName, @Nonnull DataType dataType,
        @Nonnull Object defaultNullValue) {
      schema.addField(new DimensionFieldSpec(dimensionName, dataType, false, defaultNullValue));
      return this;
    }

    public SchemaBuilder addMetric(@Nonnull String metricName, @Nonnull DataType dataType) {
      schema.addField(new MetricFieldSpec(metricName, dataType));
      return this;
    }

    public SchemaBuilder addMetric(@Nonnull String metricName, @Nonnull DataType dataType,
        @Nonnull Object defaultNullValue) {
      schema.addField(new MetricFieldSpec(metricName, dataType, defaultNullValue));
      return this;
    }

    public SchemaBuilder addMetric(@Nonnull String name, @Nonnull DataType dataType, int fieldSize,
        @Nonnull MetricFieldSpec.DerivedMetricType derivedMetricType) {
      schema.addField(new MetricFieldSpec(name, dataType, fieldSize, derivedMetricType));
      return this;
    }

    public SchemaBuilder addMetric(@Nonnull String name, @Nonnull DataType dataType, int fieldSize,
        @Nonnull MetricFieldSpec.DerivedMetricType derivedMetricType, @Nonnull Object defaultNullValue) {
      schema.addField(new MetricFieldSpec(name, dataType, fieldSize, derivedMetricType, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull String incomingName, @Nonnull TimeUnit incomingTimeUnit,
        @Nonnull DataType incomingDataType) {
      schema.addField(new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnit));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull String incomingName, @Nonnull TimeUnit incomingTimeUnit,
        @Nonnull DataType incomingDataType, @Nonnull Object defaultNullValue) {
      schema.addField(new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnit, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull String incomingName, @Nonnull TimeUnit incomingTimeUnit,
        @Nonnull DataType incomingDataType, @Nonnull String outgoingName, @Nonnull TimeUnit outgoingTimeUnit,
        @Nonnull DataType outgoingDataType) {
      schema.addField(
          new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnit, outgoingName, outgoingDataType,
              outgoingTimeUnit));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull String incomingName, @Nonnull TimeUnit incomingTimeUnit,
        @Nonnull DataType incomingDataType, @Nonnull String outgoingName, @Nonnull TimeUnit outgoingTimeUnit,
        @Nonnull DataType outgoingDataType, @Nonnull Object defaultNullValue) {
      schema.addField(
          new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnit, outgoingName, outgoingDataType,
              outgoingTimeUnit, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull String incomingName, int incomingTimeUnitSize,
        @Nonnull TimeUnit incomingTimeUnit, @Nonnull DataType incomingDataType) {
      schema.addField(new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnitSize, incomingTimeUnit));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull String incomingName, int incomingTimeUnitSize,
        @Nonnull TimeUnit incomingTimeUnit, @Nonnull DataType incomingDataType, @Nonnull Object defaultNullValue) {
      schema.addField(
          new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnitSize, incomingTimeUnit, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull String incomingName, int incomingTimeUnitSize,
        @Nonnull TimeUnit incomingTimeUnit, @Nonnull DataType incomingDataType, @Nonnull String outgoingName,
        int outgoingTimeUnitSize, @Nonnull TimeUnit outgoingTimeUnit, @Nonnull DataType outgoingDataType) {
      schema.addField(
          new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnitSize, incomingTimeUnit, outgoingName,
              outgoingDataType, outgoingTimeUnitSize, outgoingTimeUnit));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull String incomingName, int incomingTimeUnitSize,
        @Nonnull TimeUnit incomingTimeUnit, @Nonnull DataType incomingDataType, @Nonnull String outgoingName,
        int outgoingTimeUnitSize, @Nonnull TimeUnit outgoingTimeUnit, @Nonnull DataType outgoingDataType,
        @Nonnull Object defaultNullValue) {
      schema.addField(
          new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnitSize, incomingTimeUnit, outgoingName,
              outgoingDataType, outgoingTimeUnitSize, outgoingTimeUnit, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull TimeGranularitySpec incomingTimeGranularitySpec) {
      schema.addField(new TimeFieldSpec(incomingTimeGranularitySpec));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull TimeGranularitySpec incomingTimeGranularitySpec,
        @Nonnull Object defaultNullValue) {
      schema.addField(new TimeFieldSpec(incomingTimeGranularitySpec, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull TimeGranularitySpec incomingTimeGranularitySpec,
        @Nonnull TimeGranularitySpec outgoingTimeGranularitySpec) {
      schema.addField(new TimeFieldSpec(incomingTimeGranularitySpec, outgoingTimeGranularitySpec));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull TimeGranularitySpec incomingTimeGranularitySpec,
        @Nonnull TimeGranularitySpec outgoingTimeGranularitySpec, @Nonnull Object defaultNullValue) {
      schema.addField(new TimeFieldSpec(incomingTimeGranularitySpec, outgoingTimeGranularitySpec, defaultNullValue));
      return this;
    }

    public Schema build() {
      if (!schema.validate(LOGGER)) {
        throw new RuntimeException("Invalid schema");
      }
      return schema;
    }
  }

  @Override
  public String toString() {
    return getJSONSchema();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object instanceof Schema) {
      Schema that = (Schema) object;
      return schemaName.equals(that.schemaName) && fieldSpecMap.equals(that.fieldSpecMap);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return EqualityUtils.hashCodeOf(schemaName.hashCode(), fieldSpecMap);
  }
}
