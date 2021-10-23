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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;
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
public final class Schema implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Schema.class);

  private String _schemaName;
  private final List<DimensionFieldSpec> _dimensionFieldSpecs = new ArrayList<>();
  private final List<MetricFieldSpec> _metricFieldSpecs = new ArrayList<>();
  private TimeFieldSpec _timeFieldSpec;
  private final List<DateTimeFieldSpec> _dateTimeFieldSpecs = new ArrayList<>();
  private final List<ComplexFieldSpec> _complexFieldSpecs = new ArrayList<>();
  // names of the columns that used as primary keys
  // TODO(yupeng): add validation checks like duplicate columns and use of time column
  private List<String> _primaryKeyColumns;

  // Json ignored fields
  private final Map<String, FieldSpec> _fieldSpecMap = new HashMap<>();
  private transient final List<String> _dimensionNames = new ArrayList<>();
  private transient final List<String> _metricNames = new ArrayList<>();
  private transient final List<String> _dateTimeNames = new ArrayList<>();

  // Set to true if this schema has a JSON column (used to quickly decide whether to run JsonStatementOptimizer on
  // queries or not).
  private boolean _hasJSONColumn;

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

  public List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  public void setPrimaryKeyColumns(List<String> primaryKeyColumns) {
    _primaryKeyColumns = primaryKeyColumns;
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
      case COMPLEX:
        _complexFieldSpecs.add((ComplexFieldSpec) fieldSpec);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
    }

    _hasJSONColumn |= fieldSpec.getDataType().equals(DataType.JSON);
    _fieldSpecMap.put(columnName, fieldSpec);
  }

  @Deprecated
  // For third-eye backward compatible.
  public void addField(String columnName, FieldSpec fieldSpec) {
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

  public boolean hasColumn(String columnName) {
    return _fieldSpecMap.containsKey(columnName);
  }

  public boolean hasJSONColumn() {
    return _hasJSONColumn;
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

  /**
   * Fetches the DateTimeFieldSpec for the given time column name.
   * If the columnName is a DATE_TIME column, returns the DateTimeFieldSpec
   * If the columnName is a TIME column, converts to DateTimeFieldSpec before returning
   */
  @JsonIgnore
  @Nullable
  public DateTimeFieldSpec getSpecForTimeColumn(String timeColumnName) {
    FieldSpec fieldSpec = _fieldSpecMap.get(timeColumnName);
    if (fieldSpec != null) {
      if (fieldSpec.getFieldType() == FieldType.DATE_TIME) {
        return (DateTimeFieldSpec) fieldSpec;
      }
      if (fieldSpec.getFieldType() == FieldType.TIME) {
        return convertToDateTimeFieldSpec((TimeFieldSpec) fieldSpec);
      }
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
    if (!_complexFieldSpecs.isEmpty()) {
      ArrayNode jsonArray = JsonUtils.newArrayNode();
      for (ComplexFieldSpec complexFieldSpec : _complexFieldSpecs) {
        jsonArray.add(complexFieldSpec.toJsonObject());
      }
      jsonObject.set("complexFieldSpecs", jsonArray);
    }
    if (_primaryKeyColumns != null && !_primaryKeyColumns.isEmpty()) {
      ArrayNode jsonArray = JsonUtils.newArrayNode();
      for (String column : _primaryKeyColumns) {
        jsonArray.add(column);
      }
      jsonObject.set("primaryKeyColumns", jsonArray);
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
   *   <li>For dimension, time, date time fields, support {@link DataType}: INT, LONG, FLOAT, DOUBLE, BOOLEAN,
   *   TIMESTAMP, STRING, BYTES</li>
   *   <li>For metric fields, support {@link DataType}: INT, LONG, FLOAT, DOUBLE, BYTES</li>
   * </ul>
   */
  public void validate() {
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
            case BOOLEAN:
            case TIMESTAMP:
            case STRING:
            case JSON:
            case BYTES:
            case BIGDECIMAL:
              break;
            default:
              throw new IllegalStateException(
                  "Unsupported data type: " + dataType + " in DIMENSION/TIME field: " + fieldName);
          }
          break;
        case METRIC:
          switch (dataType) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BYTES:
            case BIGDECIMAL:
              break;
            default:
              throw new IllegalStateException("Unsupported data type: " + dataType + " in METRIC field: " + fieldName);
          }
          break;
        case COMPLEX:
          switch (dataType) {
            case STRUCT:
            case MAP:
            case LIST:
              break;
            default:
              throw new IllegalStateException("Unsupported data type: " + dataType + " in COMPLEX field: " + fieldName);
          }
          break;
        default:
          throw new IllegalStateException("Unsupported data type: " + dataType + " for field: " + fieldName);
      }
    }
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

    /**
     * Add single value dimensionFieldSpec
     */
    public SchemaBuilder addSingleValueDimension(String dimensionName, DataType dataType) {
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, true));
      return this;
    }

    /**
     * Add single value dimensionFieldSpec with a defaultNullValue
     */
    public SchemaBuilder addSingleValueDimension(String dimensionName, DataType dataType, Object defaultNullValue) {
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, true, defaultNullValue));
      return this;
    }

    /**
     * Add single value dimensionFieldSpec with maxLength and a defaultNullValue
     */
    public SchemaBuilder addSingleValueDimension(String dimensionName, DataType dataType, int maxLength,
        Object defaultNullValue) {
      Preconditions
          .checkArgument(dataType == DataType.STRING || dataType == DataType.BIGDECIMAL,
              "The maxLength field only applies to STRING, BIGDECIMAL field right now");
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, true, maxLength, defaultNullValue));
      return this;
    }

    /**
     * Add single value dimensionFieldSpec with maxLength, scale and a defaultNullValue
     */
    public SchemaBuilder addSingleValueDimension(String dimensionName, DataType dataType, int maxLength, int scale,
        Object defaultNullValue) {
      Preconditions
          .checkArgument(dataType == DataType.BIGDECIMAL,
              "The maxLength and scale fields only applies to BIGDECIMAL field right now");
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, true, maxLength, scale, defaultNullValue));
      return this;
    }
    /**
     * Add multi value dimensionFieldSpec
     */
    public SchemaBuilder addMultiValueDimension(String dimensionName, DataType dataType) {
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, false));
      return this;
    }

    /**
     * Add multi value dimensionFieldSpec with defaultNullValue
     */
    public SchemaBuilder addMultiValueDimension(String dimensionName, DataType dataType, Object defaultNullValue) {
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, false, defaultNullValue));
      return this;
    }

    /**
     * Add multi value dimensionFieldSpec with maxLength and a defaultNullValue
     */
    public SchemaBuilder addMultiValueDimension(String dimensionName, DataType dataType, int maxLength,
        Object defaultNullValue) {
      Preconditions
          .checkArgument(dataType == DataType.STRING || dataType == DataType.BIGDECIMAL,
              "The maxLength field only applies to STRING and BIGDECIMAL field right now");
      _schema.addField(new DimensionFieldSpec(dimensionName, dataType, false, maxLength, defaultNullValue));
      return this;
    }

    /**
     * Add metricFieldSpec
     */
    public SchemaBuilder addMetric(String metricName, DataType dataType) {
      _schema.addField(new MetricFieldSpec(metricName, dataType));
      return this;
    }

    /**
     * Add metricFieldSpec with defaultNullValue
     */
    public SchemaBuilder addMetric(String metricName, DataType dataType, Object defaultNullValue) {
      _schema.addField(new MetricFieldSpec(metricName, dataType, defaultNullValue));
      return this;
    }

    /**
     * Add metricFieldSpec with maxLength and defaultNullValue
     */
    public SchemaBuilder addMetric(String metricName, DataType dataType, int maxLength, Object defaultNullValue) {
      Preconditions
          .checkArgument(dataType == DataType.BIGDECIMAL,
              "The maxLength field only applies to BIGDECIMAL field right now");
      _schema.addField(new MetricFieldSpec(metricName, dataType, maxLength, defaultNullValue));
      return this;
    }

    /**
     * Add metricFieldSpec with maxLength and defaultNullValue
     */
    public SchemaBuilder addMetric(String metricName, DataType dataType, int maxLength, int scale, Object defaultNullValue) {
      Preconditions
          .checkArgument(dataType == DataType.BIGDECIMAL,
              "The maxLength and scale fields only applies to BIGDECIMAL field right now");
      _schema.addField(new MetricFieldSpec(metricName, dataType, maxLength, scale, defaultNullValue));
      return this;
    }

    /**
     * @deprecated in favor of {@link SchemaBuilder#addDateTime(String, DataType, String, String)}
     * Adds timeFieldSpec with incoming and outgoing granularity spec
     * This will continue to exist for a while in several tests, as it helps to test backward compatibility of
     * schemas containing
     * TimeFieldSpec
     */
    @Deprecated
    public SchemaBuilder addTime(TimeGranularitySpec incomingTimeGranularitySpec,
        @Nullable TimeGranularitySpec outgoingTimeGranularitySpec) {
      if (outgoingTimeGranularitySpec != null) {
        _schema.addField(new TimeFieldSpec(incomingTimeGranularitySpec, outgoingTimeGranularitySpec));
      } else {
        _schema.addField(new TimeFieldSpec(incomingTimeGranularitySpec));
      }
      return this;
    }

    /**
     * Add dateTimeFieldSpec with basic fields
     */
    public SchemaBuilder addDateTime(String name, DataType dataType, String format, String granularity) {
      _schema.addField(new DateTimeFieldSpec(name, dataType, format, granularity));
      return this;
    }

    /**
     * Add dateTimeFieldSpec with basic fields plus defaultNullValue and transformFunction
     */
    public SchemaBuilder addDateTime(String name, DataType dataType, String format, String granularity,
        @Nullable Object defaultNullValue, @Nullable String transformFunction) {
      DateTimeFieldSpec dateTimeFieldSpec =
          new DateTimeFieldSpec(name, dataType, format, granularity, defaultNullValue, transformFunction);
      _schema.addField(dateTimeFieldSpec);
      return this;
    }

    /**
     * Add complex field spec
     * @param name name of complex (nested) field
     * @param dataType root data type of complex field
     */
    public SchemaBuilder addComplex(String name, DataType dataType) {
      _schema.addField(new ComplexFieldSpec(name, dataType, /* single value field */ true));
      return this;
    }

    public SchemaBuilder setPrimaryKeyColumns(List<String> primaryKeyColumns) {
      _schema.setPrimaryKeyColumns(primaryKeyColumns);
      return this;
    }

    public Schema build() {
      try {
        _schema.validate();
      } catch (Exception e) {
        throw new RuntimeException("Invalid schema", e);
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
        .isEqualIgnoreOrder(_dateTimeFieldSpecs, that._dateTimeFieldSpecs) && EqualityUtils
        .isEqualIgnoreOrder(_complexFieldSpecs, that._complexFieldSpecs) && EqualityUtils
        .isEqualMap(_fieldSpecMap, that._fieldSpecMap) && EqualityUtils
        .isEqual(_primaryKeyColumns, that._primaryKeyColumns) && EqualityUtils
        .isEqual(_hasJSONColumn, that._hasJSONColumn);
  }

  /**
   * Updates fields with BOOLEAN data type to STRING if the data type in the old schema is STRING.
   *
   * BOOLEAN data type was stored as STRING within the schema before release 0.8.0. In release 0.8.0, we introduced
   * native BOOLEAN support and BOOLEAN data type is no longer replaced with STRING.
   * To keep the existing schema backward compatible, when the new field spec has BOOLEAN data type and the old field
   * spec has STRING data type, set the new field spec's data type to STRING.
   */
  public void updateBooleanFieldsIfNeeded(Schema oldSchema) {
    for (Map.Entry<String, FieldSpec> entry : _fieldSpecMap.entrySet()) {
      FieldSpec fieldSpec = entry.getValue();
      if (fieldSpec.getDataType() == DataType.BOOLEAN) {
        FieldSpec oldFieldSpec = oldSchema.getFieldSpecFor(entry.getKey());
        if (oldFieldSpec != null && oldFieldSpec.getDataType() == DataType.STRING) {
          fieldSpec.setDataType(DataType.STRING);
        }
      }
    }
  }

  /**
   * Check whether the current schema is backward compatible with oldSchema.
   * Backward compatibility requires all columns and fieldSpec in oldSchema should be retained.
   *
   * @param oldSchema old schema
   */
  public boolean isBackwardCompatibleWith(Schema oldSchema) {
    Set<String> columnNames = getColumnNames();
    for (Map.Entry<String, FieldSpec> entry : oldSchema.getFieldSpecMap().entrySet()) {
      String oldSchemaColumnName = entry.getKey();
      if (!columnNames.contains(oldSchemaColumnName)) {
        return false;
      }
      FieldSpec oldSchemaFieldSpec = entry.getValue();
      FieldSpec fieldSpec = getFieldSpecFor(oldSchemaColumnName);
      if (!fieldSpec.equals(oldSchemaFieldSpec)) {
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
    result = EqualityUtils.hashCodeOf(result, _complexFieldSpecs);
    result = EqualityUtils.hashCodeOf(result, _fieldSpecMap);
    result = EqualityUtils.hashCodeOf(result, _primaryKeyColumns);
    result = EqualityUtils.hashCodeOf(result, _hasJSONColumn);
    return result;
  }

  /**
   * Helper method that converts a {@link TimeFieldSpec} to {@link DateTimeFieldSpec}
   * 1) If timeFieldSpec contains only incoming granularity spec, directly convert it to a dateTimeFieldSpec
   * 2) If timeFieldSpec contains incoming aas well as outgoing granularity spec, use the outgoing spec to construct
   * the dateTimeFieldSpec,
   *    and configure a transform function for the conversion from incoming
   */
  @VisibleForTesting
  static DateTimeFieldSpec convertToDateTimeFieldSpec(TimeFieldSpec timeFieldSpec) {
    DateTimeFieldSpec dateTimeFieldSpec = new DateTimeFieldSpec();
    TimeGranularitySpec incomingGranularitySpec = timeFieldSpec.getIncomingGranularitySpec();
    TimeGranularitySpec outgoingGranularitySpec = timeFieldSpec.getOutgoingGranularitySpec();

    dateTimeFieldSpec.setName(outgoingGranularitySpec.getName());
    dateTimeFieldSpec.setDataType(outgoingGranularitySpec.getDataType());

    int outgoingTimeSize = outgoingGranularitySpec.getTimeUnitSize();
    TimeUnit outgoingTimeUnit = outgoingGranularitySpec.getTimeType();
    String outgoingTimeFormat = outgoingGranularitySpec.getTimeFormat();
    String[] split = StringUtils.split(outgoingTimeFormat, DateTimeFormatSpec.COLON_SEPARATOR, 2);
    DateTimeFormatSpec formatSpec;
    if (split[0].equals(DateTimeFieldSpec.TimeFormat.EPOCH.toString())) {
      formatSpec = new DateTimeFormatSpec(outgoingTimeSize, outgoingTimeUnit.toString(), split[0]);
    } else {
      formatSpec = new DateTimeFormatSpec(outgoingTimeSize, outgoingTimeUnit.toString(), split[0], split[1]);
    }
    dateTimeFieldSpec.setFormat(formatSpec.getFormat());
    DateTimeGranularitySpec granularitySpec = new DateTimeGranularitySpec(outgoingTimeSize, outgoingTimeUnit);
    dateTimeFieldSpec.setGranularity(granularitySpec.getGranularity());

    if (timeFieldSpec.getTransformFunction() != null) {
      dateTimeFieldSpec.setTransformFunction(timeFieldSpec.getTransformFunction());
    } else if (!incomingGranularitySpec.equals(outgoingGranularitySpec)) {
      String incomingName = incomingGranularitySpec.getName();
      int incomingTimeSize = incomingGranularitySpec.getTimeUnitSize();
      TimeUnit incomingTimeUnit = incomingGranularitySpec.getTimeType();
      String incomingTimeFormat = incomingGranularitySpec.getTimeFormat();
      Preconditions.checkState(
          incomingTimeFormat.equals(DateTimeFieldSpec.TimeFormat.EPOCH.toString()) && outgoingTimeFormat
              .equals(DateTimeFieldSpec.TimeFormat.EPOCH.toString()),
          "Conversion from incoming to outgoing is not supported for SIMPLE_DATE_FORMAT");
      String transformFunction =
          constructTransformFunctionString(incomingName, incomingTimeSize, incomingTimeUnit, outgoingTimeSize,
              outgoingTimeUnit);
      dateTimeFieldSpec.setTransformFunction(transformFunction);
    }

    dateTimeFieldSpec.setMaxLength(timeFieldSpec.getMaxLength());
    dateTimeFieldSpec.setDefaultNullValue(timeFieldSpec.getDefaultNullValue());

    return dateTimeFieldSpec;
  }

  /**
   * Constructs a transformFunction string for the time column, based on incoming and outgoing timeGranularitySpec
   */
  private static String constructTransformFunctionString(String incomingName, int incomingTimeSize,
      TimeUnit incomingTimeUnit, int outgoingTimeSize, TimeUnit outgoingTimeUnit) {

    String innerFunction = incomingName;
    switch (incomingTimeUnit) {
      case MILLISECONDS:
        // do nothing
        break;
      case SECONDS:
        if (incomingTimeSize > 1) {
          innerFunction = String.format("fromEpochSecondsBucket(%s, %d)", incomingName, incomingTimeSize);
        } else {
          innerFunction = String.format("fromEpochSeconds(%s)", incomingName);
        }
        break;
      case MINUTES:
        if (incomingTimeSize > 1) {
          innerFunction = String.format("fromEpochMinutesBucket(%s, %d)", incomingName, incomingTimeSize);
        } else {
          innerFunction = String.format("fromEpochMinutes(%s)", incomingName);
        }
        break;
      case HOURS:
        if (incomingTimeSize > 1) {
          innerFunction = String.format("fromEpochHoursBucket(%s, %d)", incomingName, incomingTimeSize);
        } else {
          innerFunction = String.format("fromEpochHours(%s)", incomingName);
        }
        break;
      case DAYS:
        if (incomingTimeSize > 1) {
          innerFunction = String.format("fromEpochDaysBucket(%s, %d)", incomingName, incomingTimeSize);
        } else {
          innerFunction = String.format("fromEpochDays(%s)", incomingName);
        }
        break;
      default:
        throw new IllegalStateException("Unsupported incomingTimeUnit - " + incomingTimeUnit);
    }

    String outerFunction = innerFunction;
    switch (outgoingTimeUnit) {
      case MILLISECONDS:
        break;
      case SECONDS:
        if (outgoingTimeSize > 1) {
          outerFunction = String.format("toEpochSecondsBucket(%s, %d)", innerFunction, outgoingTimeSize);
        } else {
          outerFunction = String.format("toEpochSeconds(%s)", innerFunction);
        }
        break;
      case MINUTES:
        if (outgoingTimeSize > 1) {
          outerFunction = String.format("toEpochMinutesBucket(%s, %d)", innerFunction, outgoingTimeSize);
        } else {
          outerFunction = String.format("toEpochMinutes(%s)", innerFunction);
        }
        break;
      case HOURS:
        if (outgoingTimeSize > 1) {
          outerFunction = String.format("toEpochHoursBucket(%s, %d)", innerFunction, outgoingTimeSize);
        } else {
          outerFunction = String.format("toEpochHours(%s)", innerFunction);
        }
        break;
      case DAYS:
        if (outgoingTimeSize > 1) {
          outerFunction = String.format("toEpochDaysBucket(%s, %d)", innerFunction, outgoingTimeSize);
        } else {
          outerFunction = String.format("toEpochDays(%s)", innerFunction);
        }
        break;
      default:
        throw new IllegalStateException("Unsupported outgoingTimeUnit - " + outgoingTimeUnit);
    }
    return outerFunction;
  }
}
