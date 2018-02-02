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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.utils.EqualityUtils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>Schema</code> class is defined for each table to describe the details of the table's fields (columns).
 * <p>Four field types are supported: DIMENSION, METRIC, TIME, DATE_TIME.
 * ({@link com.linkedin.pinot.common.data.DimensionFieldSpec}, {@link com.linkedin.pinot.common.data.MetricFieldSpec},
 * {@link com.linkedin.pinot.common.data.TimeFieldSpec}, {@link com.linkedin.pinot.common.data.DateTimeFieldSpec})
 * <p>For each field, a {@link com.linkedin.pinot.common.data.FieldSpec} is defined to provide the details of the field.
 * <p>There could be multiple DIMENSION or METRIC or DATE_TIME fields, but at most 1 TIME field.
 * <p>In pinot, we store data using 5 <code>DataType</code>s: INT, LONG, FLOAT, DOUBLE, STRING. All other
 * <code>DataType</code>s will be converted to one of them.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class Schema {
  private static final Logger LOGGER = LoggerFactory.getLogger(Schema.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String _schemaName;
  private final List<DimensionFieldSpec> _dimensionFieldSpecs = new ArrayList<>();
  private final List<MetricFieldSpec> _metricFieldSpecs = new ArrayList<>();
  private TimeFieldSpec _timeFieldSpec;
  private final List<DateTimeFieldSpec> _dateTimeFieldSpecs = new ArrayList<>();

  // Json ignored fields
  private final Map<String, FieldSpec> _fieldSpecMap = new HashMap<>();
  private final List<String> _dimensionNames = new ArrayList<>();
  private final List<String> _metricNames = new ArrayList<>();
  private final List<String> _dateTimeNames = new ArrayList<>();

  @Nonnull
  public static Schema fromFile(@Nonnull File schemaFile) throws IOException {
    return MAPPER.readValue(schemaFile, Schema.class);
  }

  @Nonnull
  public static Schema fromString(@Nonnull String schemaString) throws IOException {
    return MAPPER.readValue(schemaString, Schema.class);
  }

  @Nonnull
  public static Schema fromInputSteam(@Nonnull InputStream schemaInputStream) throws IOException {
    return MAPPER.readValue(schemaInputStream, Schema.class);
  }

  /**
   * NOTE: schema name could be null in tests
   */
  public String getSchemaName() {
    return _schemaName;
  }

  public void setSchemaName(@Nonnull String schemaName) {
    _schemaName = schemaName;
  }

  @Nonnull
  public List<DimensionFieldSpec> getDimensionFieldSpecs() {
    return _dimensionFieldSpecs;
  }

  /**
   * Required by JSON deserializer. DO NOT USE. DO NOT REMOVE.
   * Adding @Deprecated to prevent usage
   * @param dimensionFieldSpecs
   */
  @Deprecated
  public void setDimensionFieldSpecs(@Nonnull List<DimensionFieldSpec> dimensionFieldSpecs) {
    Preconditions.checkState(_dimensionFieldSpecs.isEmpty());

    for (DimensionFieldSpec dimensionFieldSpec : dimensionFieldSpecs) {
      addField(dimensionFieldSpec);
    }
  }

  @Nonnull
  public List<MetricFieldSpec> getMetricFieldSpecs() {
    return _metricFieldSpecs;
  }

  /**
   * Required by JSON deserializer. DO NOT USE. DO NOT REMOVE.
   * Adding @Deprecated to prevent usage
   * @param metricFieldSpecs
   */
  @Deprecated
  public void setMetricFieldSpecs(@Nonnull List<MetricFieldSpec> metricFieldSpecs) {
    Preconditions.checkState(_metricFieldSpecs.isEmpty());

    for (MetricFieldSpec metricFieldSpec : metricFieldSpecs) {
      addField(metricFieldSpec);
    }
  }

  @Nonnull
  public List<DateTimeFieldSpec> getDateTimeFieldSpecs() {
    return _dateTimeFieldSpecs;
  }

  /**
   * Required by JSON deserializer. DO NOT USE. DO NOT REMOVE.
   * Adding @Deprecated to prevent usage
   * @param dateTimeFieldSpecs
   */
  @Deprecated
  public void setDateTimeFieldSpecs(@Nonnull List<DateTimeFieldSpec> dateTimeFieldSpecs) {
    Preconditions.checkState(_dateTimeFieldSpecs.isEmpty());

    for (DateTimeFieldSpec dateTimeFieldSpec : dateTimeFieldSpecs) {
      addField(dateTimeFieldSpec);
    }
  }

  @Nullable
  public TimeFieldSpec getTimeFieldSpec() {
    return _timeFieldSpec;
  }

  /**
   * Required by JSON deserializer. DO NOT USE. DO NOT REMOVE.
   * Adding @Deprecated to prevent usage
   * @param timeFieldSpec
   */
  @Deprecated
  public void setTimeFieldSpec(@Nullable TimeFieldSpec timeFieldSpec) {
    if (timeFieldSpec != null) {
      addField(timeFieldSpec);
    }
  }

  public void addField(@Nonnull FieldSpec fieldSpec) {
    Preconditions.checkNotNull(fieldSpec);
    String columnName = fieldSpec.getName();
    Preconditions.checkNotNull(columnName);
    Preconditions.checkState(!_fieldSpecMap.containsKey(columnName),
        "Field spec already exists for column: " + columnName);

    FieldType fieldType = fieldSpec.getFieldType();
    switch (fieldType) {
      case DIMENSION:
        _dimensionNames.add(columnName);
        _dimensionFieldSpecs.add((DimensionFieldSpec) fieldSpec);
        break;
      case METRIC:
        _metricNames.add(columnName);
        _metricFieldSpecs.add((MetricFieldSpec) fieldSpec);
        break;
      case TIME:
        _timeFieldSpec = (TimeFieldSpec) fieldSpec;
        break;
      case DATE_TIME:
        _dateTimeNames.add(columnName);
        _dateTimeFieldSpecs.add((DateTimeFieldSpec) fieldSpec);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
    }

    _fieldSpecMap.put(columnName, fieldSpec);
  }

  @Deprecated
  // For third-eye backward compatible.
  public void addField(@Nonnull String columnName, @Nonnull FieldSpec fieldSpec) {
    addField(fieldSpec);
  }

  public boolean removeField(String columnName) {
    FieldSpec existingFieldSpec = _fieldSpecMap.remove(columnName);
    if (existingFieldSpec != null) {
      FieldType fieldType = existingFieldSpec.getFieldType();
      switch (fieldType) {
        case DIMENSION:
          int index = _dimensionNames.indexOf(columnName);
          _dimensionNames.remove(index);
          _dimensionFieldSpecs.remove(index);
          break;
        case METRIC:
          index = _metricNames.indexOf(columnName);
          _metricNames.remove(index);
          _metricFieldSpecs.remove(index);
          break;
        case TIME:
          _timeFieldSpec = null;
          break;
        case DATE_TIME:
          index = _dateTimeNames.indexOf(columnName);
          _dateTimeNames.remove(index);
          _dateTimeFieldSpecs.remove(index);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
      }
      return true;
    } else {
      return false;
    }
  }

  public boolean hasColumn(@Nonnull String columnName) {
    return _fieldSpecMap.containsKey(columnName);
  }

  @JsonIgnore
  @Nonnull
  public Map<String, FieldSpec> getFieldSpecMap() {
    return _fieldSpecMap;
  }

  @JsonIgnore
  @Nonnull
  public Set<String> getColumnNames() {
    return _fieldSpecMap.keySet();
  }

  @JsonIgnore
  @Nonnull
  public Collection<FieldSpec> getAllFieldSpecs() {
    return _fieldSpecMap.values();
  }

  public int size() {
    return _fieldSpecMap.size();
  }

  @JsonIgnore
  @Nullable
  public FieldSpec getFieldSpecFor(@Nonnull String columnName) {
    return _fieldSpecMap.get(columnName);
  }

  @JsonIgnore
  @Nullable
  public MetricFieldSpec getMetricSpec(@Nonnull String metricName) {
    FieldSpec fieldSpec = _fieldSpecMap.get(metricName);
    if (fieldSpec != null && fieldSpec.getFieldType() == FieldType.METRIC) {
      return (MetricFieldSpec) fieldSpec;
    }
    return null;
  }

  @JsonIgnore
  @Nullable
  public DimensionFieldSpec getDimensionSpec(@Nonnull String dimensionName) {
    FieldSpec fieldSpec = _fieldSpecMap.get(dimensionName);
    if (fieldSpec != null && fieldSpec.getFieldType() == FieldType.DIMENSION) {
      return (DimensionFieldSpec) fieldSpec;
    }
    return null;
  }

  @JsonIgnore
  @Nullable
  public DateTimeFieldSpec getDateTimeSpec(@Nonnull String dateTimeName) {
    FieldSpec fieldSpec = _fieldSpecMap.get(dateTimeName);
    if (fieldSpec != null && fieldSpec.getFieldType() == FieldType.DATE_TIME) {
      return (DateTimeFieldSpec) fieldSpec;
    }
    return null;
  }

  @JsonIgnore
  @Nonnull
  public List<String> getDimensionNames() {
    return _dimensionNames;
  }

  @JsonIgnore
  @Nonnull
  public List<String> getMetricNames() {
    return _metricNames;
  }

  @JsonIgnore
  @Nonnull
  public List<String> getDateTimeNames() {
    return _dateTimeNames;
  }

  @JsonIgnore
  @Nullable
  public String getTimeColumnName() {
    return (_timeFieldSpec != null) ? _timeFieldSpec.getName() : null;
  }

  @JsonIgnore
  @Nullable
  public TimeUnit getIncomingTimeUnit() {
    return (_timeFieldSpec != null) ? _timeFieldSpec.getIncomingGranularitySpec().getTimeType() : null;
  }

  @JsonIgnore
  @Nullable
  public TimeUnit getOutgoingTimeUnit() {
    return (_timeFieldSpec != null) ? _timeFieldSpec.getOutgoingGranularitySpec().getTimeType() : null;
  }

  @JsonIgnore
  @Nonnull
  public String getJSONSchema() {
    try {
      return MAPPER.writeValueAsString(this);
    } catch (IOException e) {
      throw new RuntimeException("Caught exception while writing Schema as JSON format string.", e);
    }
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
    for (FieldSpec fieldSpec : _fieldSpecMap.values()) {
      FieldType fieldType = fieldSpec.getFieldType();
      DataType dataType = fieldSpec.getDataType();
      String fieldName = fieldSpec.getName();
      try {
        switch (fieldType) {
          case DIMENSION:
          case TIME:
          case DATE_TIME:
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
    private Schema _schema;

    public SchemaBuilder() {
      _schema = new Schema();
    }

    public SchemaBuilder setSchemaName(@Nonnull String schemaName) {
      _schema.setSchemaName(schemaName);
      return this;
    }

    public SchemaBuilder addSingleValueDimension(@Nonnull String dimensionName, @Nonnull DataType dataType) {
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, true));
      return this;
    }

    public SchemaBuilder addSingleValueDimension(@Nonnull String dimensionName, @Nonnull DataType dataType,
        @Nonnull Object defaultNullValue) {
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, true, defaultNullValue));
      return this;
    }

    public SchemaBuilder addMultiValueDimension(@Nonnull String dimensionName, @Nonnull DataType dataType) {
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, false));
      return this;
    }

    public SchemaBuilder addMultiValueDimension(@Nonnull String dimensionName, @Nonnull DataType dataType,
        @Nonnull Object defaultNullValue) {
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, false, defaultNullValue));
      return this;
    }

    public SchemaBuilder addMetric(@Nonnull String metricName, @Nonnull DataType dataType) {
      _schema.addField(new MetricFieldSpec(metricName, dataType));
      return this;
    }

    public SchemaBuilder addMetric(@Nonnull String metricName, @Nonnull DataType dataType,
        @Nonnull Object defaultNullValue) {
      _schema.addField(new MetricFieldSpec(metricName, dataType, defaultNullValue));
      return this;
    }

    public SchemaBuilder addMetric(@Nonnull String name, @Nonnull DataType dataType, int fieldSize,
        @Nonnull MetricFieldSpec.DerivedMetricType derivedMetricType) {
      _schema.addField(new MetricFieldSpec(name, dataType, fieldSize, derivedMetricType));
      return this;
    }

    public SchemaBuilder addMetric(@Nonnull String name, @Nonnull DataType dataType, int fieldSize,
        @Nonnull MetricFieldSpec.DerivedMetricType derivedMetricType, @Nonnull Object defaultNullValue) {
      _schema.addField(new MetricFieldSpec(name, dataType, fieldSize, derivedMetricType, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull String incomingName, @Nonnull TimeUnit incomingTimeUnit,
        @Nonnull DataType incomingDataType) {
      _schema.addField(new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnit));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull String incomingName, @Nonnull TimeUnit incomingTimeUnit,
        @Nonnull DataType incomingDataType, @Nonnull Object defaultNullValue) {
      _schema.addField(new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnit, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull String incomingName, @Nonnull TimeUnit incomingTimeUnit,
        @Nonnull DataType incomingDataType, @Nonnull String outgoingName, @Nonnull TimeUnit outgoingTimeUnit,
        @Nonnull DataType outgoingDataType) {
      _schema.addField(
          new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnit, outgoingName, outgoingDataType,
              outgoingTimeUnit));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull String incomingName, @Nonnull TimeUnit incomingTimeUnit,
        @Nonnull DataType incomingDataType, @Nonnull String outgoingName, @Nonnull TimeUnit outgoingTimeUnit,
        @Nonnull DataType outgoingDataType, @Nonnull Object defaultNullValue) {
      _schema.addField(
          new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnit, outgoingName, outgoingDataType,
              outgoingTimeUnit, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull String incomingName, int incomingTimeUnitSize,
        @Nonnull TimeUnit incomingTimeUnit, @Nonnull DataType incomingDataType) {
      _schema.addField(new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnitSize, incomingTimeUnit));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull String incomingName, int incomingTimeUnitSize,
        @Nonnull TimeUnit incomingTimeUnit, @Nonnull DataType incomingDataType, @Nonnull Object defaultNullValue) {
      _schema.addField(
          new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnitSize, incomingTimeUnit, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull String incomingName, int incomingTimeUnitSize,
        @Nonnull TimeUnit incomingTimeUnit, @Nonnull DataType incomingDataType, @Nonnull String outgoingName,
        int outgoingTimeUnitSize, @Nonnull TimeUnit outgoingTimeUnit, @Nonnull DataType outgoingDataType) {
      _schema.addField(
          new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnitSize, incomingTimeUnit, outgoingName,
              outgoingDataType, outgoingTimeUnitSize, outgoingTimeUnit));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull String incomingName, int incomingTimeUnitSize,
        @Nonnull TimeUnit incomingTimeUnit, @Nonnull DataType incomingDataType, @Nonnull String outgoingName,
        int outgoingTimeUnitSize, @Nonnull TimeUnit outgoingTimeUnit, @Nonnull DataType outgoingDataType,
        @Nonnull Object defaultNullValue) {
      _schema.addField(
          new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnitSize, incomingTimeUnit, outgoingName,
              outgoingDataType, outgoingTimeUnitSize, outgoingTimeUnit, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull TimeGranularitySpec incomingTimeGranularitySpec) {
      _schema.addField(new TimeFieldSpec(incomingTimeGranularitySpec));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull TimeGranularitySpec incomingTimeGranularitySpec,
        @Nonnull Object defaultNullValue) {
      _schema.addField(new TimeFieldSpec(incomingTimeGranularitySpec, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull TimeGranularitySpec incomingTimeGranularitySpec,
        @Nonnull TimeGranularitySpec outgoingTimeGranularitySpec) {
      _schema.addField(new TimeFieldSpec(incomingTimeGranularitySpec, outgoingTimeGranularitySpec));
      return this;
    }

    public SchemaBuilder addTime(@Nonnull TimeGranularitySpec incomingTimeGranularitySpec,
        @Nonnull TimeGranularitySpec outgoingTimeGranularitySpec, @Nonnull Object defaultNullValue) {
      _schema.addField(new TimeFieldSpec(incomingTimeGranularitySpec, outgoingTimeGranularitySpec, defaultNullValue));
      return this;
    }

    public SchemaBuilder addDateTime(@Nonnull String name, @Nonnull DataType dataType, @Nonnull String format,
        @Nonnull String granularity) {
      _schema.addField(new DateTimeFieldSpec(name, dataType, format, granularity));
      return this;
    }

    public Schema build() {
      if (!_schema.validate(LOGGER)) {
        throw new RuntimeException("Invalid schema");
      }
      return _schema;
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
      return _schemaName.equals(that._schemaName) && _fieldSpecMap.equals(that._fieldSpecMap);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return EqualityUtils.hashCodeOf(_schemaName.hashCode(), _fieldSpecMap);
  }
}
