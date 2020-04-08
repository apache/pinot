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
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
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
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>Schema</code> class is defined for each table to describe the details of the table's fields (columns).
 * <p>Four field types are supported: DIMENSION, METRIC, TIME, DATE_TIME.
 * ({@link DimensionFieldSpec}, {@link MetricFieldSpec},
 * {@link TimeFieldSpec}, {@link DateTimeFieldSpec})
 * <p>For each field, a {@link FieldSpec} is defined to provide the details of the field.
 * <p>There could be multiple DIMENSION or METRIC or DATE_TIME fields, but at most 1 TIME field.
 * <p>In pinot, we store data using 5 <code>DataType</code>s: INT, LONG, FLOAT, DOUBLE, STRING. All other
 * <code>DataType</code>s will be converted to one of them.
 */
@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public final class Schema {
  private static final Logger LOGGER = LoggerFactory.getLogger(Schema.class);

  private String _schemaName;
  private final List<DimensionFieldSpec> _dimensionFieldSpecs = new ArrayList<>();
  private final List<MetricFieldSpec> _metricFieldSpecs = new ArrayList<>();
  private TimeFieldSpec _timeFieldSpec;
  private final List<DateTimeFieldSpec> _dateTimeFieldSpecs = new ArrayList<>();

  // Json ignored fields
  private transient final Map<String, FieldSpec> _fieldSpecMap = new HashMap<>();
  private transient final Map<String, FieldSpec> _physicalFieldSpecMap = new HashMap<>();
  private transient final List<String> _dimensionNames = new ArrayList<>();
  private transient final List<String> _metricNames = new ArrayList<>();
  private transient final List<String> _dateTimeNames = new ArrayList<>();


  public static Schema fromFile(File schemaFile)
      throws IOException {
    return JsonUtils.fileToObject(schemaFile, Schema.class);
  }

  public static Schema fromString(String schemaString)
      throws IOException {
    return JsonUtils.stringToObject(schemaString, Schema.class);
  }

  public static Schema fromInputSteam(InputStream schemaInputStream)
      throws IOException {
    return JsonUtils.inputStreamToObject(schemaInputStream, Schema.class);
  }

  /**
   * NOTE: schema name could be null in tests
   */
  public String getSchemaName() {
    return _schemaName;
  }

  public void setSchemaName(String schemaName) {
    _schemaName = schemaName;
  }

  public List<DimensionFieldSpec> getDimensionFieldSpecs() {
    return _dimensionFieldSpecs;
  }

  /**
   * Required by JSON deserializer. DO NOT USE. DO NOT REMOVE.
   * Adding @Deprecated to prevent usage
   */
  @Deprecated
  public void setDimensionFieldSpecs(List<DimensionFieldSpec> dimensionFieldSpecs) {
    Preconditions.checkState(_dimensionFieldSpecs.isEmpty());

    for (DimensionFieldSpec dimensionFieldSpec : dimensionFieldSpecs) {
      addField(dimensionFieldSpec);
    }
  }

  public List<MetricFieldSpec> getMetricFieldSpecs() {
    return _metricFieldSpecs;
  }

  /**
   * Required by JSON deserializer. DO NOT USE. DO NOT REMOVE.
   * Adding @Deprecated to prevent usage
   */
  @Deprecated
  public void setMetricFieldSpecs(List<MetricFieldSpec> metricFieldSpecs) {
    Preconditions.checkState(_metricFieldSpecs.isEmpty());

    for (MetricFieldSpec metricFieldSpec : metricFieldSpecs) {
      addField(metricFieldSpec);
    }
  }

  public List<DateTimeFieldSpec> getDateTimeFieldSpecs() {
    return _dateTimeFieldSpecs;
  }

  /**
   * Required by JSON deserializer. DO NOT USE. DO NOT REMOVE.
   * Adding @Deprecated to prevent usage
   */
  @Deprecated
  public void setDateTimeFieldSpecs(List<DateTimeFieldSpec> dateTimeFieldSpecs) {
    Preconditions.checkState(_dateTimeFieldSpecs.isEmpty());

    for (DateTimeFieldSpec dateTimeFieldSpec : dateTimeFieldSpecs) {
      addField(dateTimeFieldSpec);
    }
  }

  public TimeFieldSpec getTimeFieldSpec() {
    return _timeFieldSpec;
  }

  /**
   * Required by JSON deserializer. DO NOT USE. DO NOT REMOVE.
   * Adding @Deprecated to prevent usage
   */
  @Deprecated
  public void setTimeFieldSpec(TimeFieldSpec timeFieldSpec) {
    if (timeFieldSpec != null) {
      addField(timeFieldSpec);
    }
  }

  public void addField(FieldSpec fieldSpec) {
    Preconditions.checkNotNull(fieldSpec);
    String columnName = fieldSpec.getName();
    Preconditions.checkNotNull(columnName);
    Preconditions
        .checkState(!_fieldSpecMap.containsKey(columnName), "Field spec already exists for column: " + columnName);

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
    if (!fieldSpec.isVirtualColumnField()) {
      _physicalFieldSpecMap.put(columnName, fieldSpec);
    }
  }

  @Deprecated
  // For third-eye backward compatible.
  public void addField(String columnName, FieldSpec fieldSpec) {
    addField(fieldSpec);
  }

  public boolean removeField(String columnName) {
    FieldSpec existingFieldSpec = _fieldSpecMap.remove(columnName);
    if (_physicalFieldSpecMap.containsKey(columnName)) {
      _physicalFieldSpecMap.remove(columnName);
    }
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

  public boolean hasColumn(String columnName) {
    return _fieldSpecMap.containsKey(columnName);
  }

  @JsonIgnore
  public Map<String, FieldSpec> getFieldSpecMap() {
    return _fieldSpecMap;
  }

  @JsonIgnore
  public Set<String> getColumnNames() {
    return _fieldSpecMap.keySet();
  }

  @JsonIgnore
  public Set<String> getPhysicalColumnNames() {
    Set<String> physicalColumnNames = new HashSet<>();
    for (FieldSpec fieldSpec : _fieldSpecMap.values()) {
      if (!fieldSpec.isVirtualColumn()) {
        physicalColumnNames.add(fieldSpec.getName());
      }
    }
    return physicalColumnNames;
  }

  @JsonIgnore
  public Collection<FieldSpec> getAllFieldSpecs() {
    return _fieldSpecMap.values();
  }

  @JsonIgnore
  @Nonnull
  public Collection<FieldSpec> getAllPhysicalFieldSpecs() {
    return _physicalFieldSpecMap.values();
  }

  public int size() {
    return _fieldSpecMap.size();
  }

  @JsonIgnore
  public FieldSpec getFieldSpecFor(String columnName) {
    return _fieldSpecMap.get(columnName);
  }

  @JsonIgnore
  public MetricFieldSpec getMetricSpec(String metricName) {
    FieldSpec fieldSpec = _fieldSpecMap.get(metricName);
    if (fieldSpec != null && fieldSpec.getFieldType() == FieldType.METRIC) {
      return (MetricFieldSpec) fieldSpec;
    }
    return null;
  }

  @JsonIgnore
  public DimensionFieldSpec getDimensionSpec(String dimensionName) {
    FieldSpec fieldSpec = _fieldSpecMap.get(dimensionName);
    if (fieldSpec != null && fieldSpec.getFieldType() == FieldType.DIMENSION) {
      return (DimensionFieldSpec) fieldSpec;
    }
    return null;
  }

  @JsonIgnore
  public DateTimeFieldSpec getDateTimeSpec(String dateTimeName) {
    FieldSpec fieldSpec = _fieldSpecMap.get(dateTimeName);
    if (fieldSpec != null && fieldSpec.getFieldType() == FieldType.DATE_TIME) {
      return (DateTimeFieldSpec) fieldSpec;
    }
    return null;
  }

  @JsonIgnore
  public List<String> getDimensionNames() {
    return _dimensionNames;
  }

  @JsonIgnore
  public List<String> getMetricNames() {
    return _metricNames;
  }

  @JsonIgnore
  public List<String> getDateTimeNames() {
    return _dateTimeNames;
  }

  @JsonIgnore
  public String getTimeColumnName() {
    return (_timeFieldSpec != null) ? _timeFieldSpec.getName() : null;
  }

  @JsonIgnore
  public TimeUnit getIncomingTimeUnit() {
    return (_timeFieldSpec != null) ? _timeFieldSpec.getIncomingGranularitySpec().getTimeType() : null;
  }

  @JsonIgnore
  public TimeUnit getOutgoingTimeUnit() {
    return (_timeFieldSpec != null) ? _timeFieldSpec.getOutgoingGranularitySpec().getTimeType() : null;
  }

  /**
   * Returns a json representation of the schema.
   */
  public ObjectNode toJsonObject() {
    ObjectNode jsonObject = JsonUtils.newObjectNode();
    jsonObject.put("schemaName", _schemaName);
    if (!_dimensionFieldSpecs.isEmpty()) {
      ArrayNode jsonArray = JsonUtils.newArrayNode();
      for (DimensionFieldSpec dimensionFieldSpec : _dimensionFieldSpecs) {
        jsonArray.add(dimensionFieldSpec.toJsonObject());
      }
      jsonObject.set("dimensionFieldSpecs", jsonArray);
    }
    if (!_metricFieldSpecs.isEmpty()) {
      ArrayNode jsonArray = JsonUtils.newArrayNode();
      for (MetricFieldSpec metricFieldSpec : _metricFieldSpecs) {
        jsonArray.add(metricFieldSpec.toJsonObject());
      }
      jsonObject.set("metricFieldSpecs", jsonArray);
    }
    if (_timeFieldSpec != null) {
      jsonObject.set("timeFieldSpec", _timeFieldSpec.toJsonObject());
    }
    if (!_dateTimeFieldSpecs.isEmpty()) {
      ArrayNode jsonArray = JsonUtils.newArrayNode();
      for (DateTimeFieldSpec dateTimeFieldSpec : _dateTimeFieldSpecs) {
        jsonArray.add(dateTimeFieldSpec.toJsonObject());
      }
      jsonObject.set("dateTimeFieldSpecs", jsonArray);
    }

    return jsonObject;
  }

  /**
   * Returns a pretty json string representation of the schema.
   */
  public String toPrettyJsonString() {
    try {
      return JsonUtils.objectToPrettyString(toJsonObject());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns a single-line json string representation of the schema.
   */
  public String toSingleLineJsonString() {
    return toJsonObject().toString();
  }

  /**
   * Validates a pinot schema.
   * <p>The following validations are performed:
   * <ul>
   *   <li>For dimension, time, date time fields, support {@link DataType}: INT, LONG, FLOAT, DOUBLE, STRING</li>
   *   <li>For non-derived metric fields, support {@link DataType}: INT, LONG, FLOAT, DOUBLE</li>
   * </ul>
   *
   * @param ctxLogger Logger used to log the message (if null, the current class logger is used)
   * @return Whether schema is valid
   */
  public boolean validate(Logger ctxLogger) {
    if (ctxLogger == null) {
      ctxLogger = LOGGER;
    }

    // Log ALL the schema errors that may be present.
    for (FieldSpec fieldSpec : _fieldSpecMap.values()) {
      FieldType fieldType = fieldSpec.getFieldType();
      DataType dataType = fieldSpec.getDataType();
      String fieldName = fieldSpec.getName();
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
            case BYTES:
              break;
            default:
              ctxLogger.info("Unsupported data type: {} in DIMENSION/TIME field: {}", dataType, fieldName);
              return false;
          }
          break;
        case METRIC:
          switch (dataType) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BYTES:
              break;
            default:
              ctxLogger.info("Unsupported data type: {} in METRIC field: {}", dataType, fieldName);
              return false;
          }
          break;
        default:
          ctxLogger.info("Unsupported field type: {} for field: {}", dataType, fieldName);
          return false;
      }
    }

    return true;
  }

  public boolean isVirtualColumn(String columnName) {
    return columnName.startsWith("$") || (getFieldSpecFor(columnName).getVirtualColumnProvider() != null
        && !getFieldSpecFor(columnName).getVirtualColumnProvider().isEmpty());
  }

  public static class SchemaBuilder {
    private Schema _schema;

    public SchemaBuilder() {
      _schema = new Schema();
    }

    public SchemaBuilder setSchemaName(String schemaName) {
      _schema.setSchemaName(schemaName);
      return this;
    }

    public SchemaBuilder addSingleValueDimension(String dimensionName, DataType dataType) {
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, true));
      return this;
    }

    public SchemaBuilder addSingleValueDimension(String dimensionName, DataType dataType, Object defaultNullValue) {
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, true, defaultNullValue));
      return this;
    }

    public SchemaBuilder addMultiValueDimension(String dimensionName, DataType dataType) {
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, false));
      return this;
    }

    public SchemaBuilder addMultiValueDimension(String dimensionName, DataType dataType, Object defaultNullValue) {
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, false, defaultNullValue));
      return this;
    }

    public SchemaBuilder addMetric(String metricName, DataType dataType) {
      _schema.addField(new MetricFieldSpec(metricName, dataType));
      return this;
    }

    public SchemaBuilder addMetric(String metricName, DataType dataType, Object defaultNullValue) {
      _schema.addField(new MetricFieldSpec(metricName, dataType, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(String incomingName, TimeUnit incomingTimeUnit, DataType incomingDataType) {
      _schema.addField(new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnit));
      return this;
    }

    public SchemaBuilder addTime(String incomingName, TimeUnit incomingTimeUnit, DataType incomingDataType,
        Object defaultNullValue) {
      _schema.addField(new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnit, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(String incomingName, TimeUnit incomingTimeUnit, DataType incomingDataType,
        String outgoingName, TimeUnit outgoingTimeUnit, DataType outgoingDataType) {
      _schema.addField(
          new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnit, outgoingName, outgoingDataType,
              outgoingTimeUnit));
      return this;
    }

    public SchemaBuilder addTime(String incomingName, TimeUnit incomingTimeUnit, DataType incomingDataType,
        String outgoingName, TimeUnit outgoingTimeUnit, DataType outgoingDataType, Object defaultNullValue) {
      _schema.addField(
          new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnit, outgoingName, outgoingDataType,
              outgoingTimeUnit, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(String incomingName, int incomingTimeUnitSize, TimeUnit incomingTimeUnit,
        DataType incomingDataType) {
      _schema.addField(new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnitSize, incomingTimeUnit));
      return this;
    }

    public SchemaBuilder addTime(String incomingName, int incomingTimeUnitSize, TimeUnit incomingTimeUnit,
        DataType incomingDataType, Object defaultNullValue) {
      _schema.addField(
          new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnitSize, incomingTimeUnit, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(String incomingName, int incomingTimeUnitSize, TimeUnit incomingTimeUnit,
        DataType incomingDataType, String outgoingName, int outgoingTimeUnitSize, TimeUnit outgoingTimeUnit,
        DataType outgoingDataType) {
      _schema.addField(
          new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnitSize, incomingTimeUnit, outgoingName,
              outgoingDataType, outgoingTimeUnitSize, outgoingTimeUnit));
      return this;
    }

    public SchemaBuilder addTime(String incomingName, int incomingTimeUnitSize, TimeUnit incomingTimeUnit,
        DataType incomingDataType, String outgoingName, int outgoingTimeUnitSize, TimeUnit outgoingTimeUnit,
        DataType outgoingDataType, Object defaultNullValue) {
      _schema.addField(
          new TimeFieldSpec(incomingName, incomingDataType, incomingTimeUnitSize, incomingTimeUnit, outgoingName,
              outgoingDataType, outgoingTimeUnitSize, outgoingTimeUnit, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(TimeGranularitySpec incomingTimeGranularitySpec) {
      _schema.addField(new TimeFieldSpec(incomingTimeGranularitySpec));
      return this;
    }

    public SchemaBuilder addTime(TimeGranularitySpec incomingTimeGranularitySpec, Object defaultNullValue) {
      _schema.addField(new TimeFieldSpec(incomingTimeGranularitySpec, defaultNullValue));
      return this;
    }

    public SchemaBuilder addTime(TimeGranularitySpec incomingTimeGranularitySpec,
        TimeGranularitySpec outgoingTimeGranularitySpec) {
      _schema.addField(new TimeFieldSpec(incomingTimeGranularitySpec, outgoingTimeGranularitySpec));
      return this;
    }

    public SchemaBuilder addTime(TimeGranularitySpec incomingTimeGranularitySpec,
        TimeGranularitySpec outgoingTimeGranularitySpec, Object defaultNullValue) {
      _schema.addField(new TimeFieldSpec(incomingTimeGranularitySpec, outgoingTimeGranularitySpec, defaultNullValue));
      return this;
    }

    public SchemaBuilder addDateTime(String name, DataType dataType, String format, String granularity) {
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
    return toPrettyJsonString();
  }

  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    Schema that = (Schema) o;

    return EqualityUtils.isEqual(_schemaName, that._schemaName) && EqualityUtils
        .isEqualIgnoreOrder(_dimensionFieldSpecs, that._dimensionFieldSpecs) && EqualityUtils
        .isEqualIgnoreOrder(_metricFieldSpecs, that._metricFieldSpecs) && EqualityUtils
        .isEqual(_timeFieldSpec, that._timeFieldSpec) && EqualityUtils
        .isEqualIgnoreOrder(_dateTimeFieldSpecs, that._dateTimeFieldSpecs);

  }

  /**
   * Check whether the current schema is backward compatible with oldSchema.
   * Backward compatibility requires all columns and fieldSpec in oldSchema should be retained.
   *
   * @param oldSchema old schema
   * @return
   */
  public boolean isBackwardCompatibleWith(Schema oldSchema) {
    if (!EqualityUtils.isEqual(_timeFieldSpec, oldSchema.getTimeFieldSpec()) || !EqualityUtils
        .isEqual(_dateTimeFieldSpecs, oldSchema.getDateTimeFieldSpecs())) {
      return false;
    }
    for (Map.Entry<String, FieldSpec> entry : oldSchema.getFieldSpecMap().entrySet()) {
      if (!getColumnNames().contains(entry.getKey())) {
        return false;
      }
      if (!getFieldSpecFor(entry.getKey()).equals(entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_schemaName);
    result = EqualityUtils.hashCodeOf(result, _dimensionFieldSpecs);
    result = EqualityUtils.hashCodeOf(result, _metricFieldSpecs);
    result = EqualityUtils.hashCodeOf(result, _timeFieldSpec);
    result = EqualityUtils.hashCodeOf(result, _dateTimeFieldSpecs);
    return result;
  }
}
