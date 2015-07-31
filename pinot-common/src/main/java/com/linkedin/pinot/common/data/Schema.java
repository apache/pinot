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

import static com.linkedin.pinot.common.utils.EqualityUtils.hashCodeOf;
import static com.linkedin.pinot.common.utils.EqualityUtils.isEqual;
import static com.linkedin.pinot.common.utils.EqualityUtils.isNullOrNotSameClass;
import static com.linkedin.pinot.common.utils.EqualityUtils.isSameReference;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.helix.ZNRecord;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 */

public class Schema {
  private static final Logger LOGGER = LoggerFactory.getLogger(Schema.class);
  private List<MetricFieldSpec> metricFieldSpecs;
  private List<DimensionFieldSpec> dimensionFieldSpecs;
  private TimeFieldSpec timeFieldSpec;
  private List<StarTreeIndexSpec> starTreeIndexSpecs;
  private String schemaName;

  @JsonIgnore(true)
  private Set<String> dimensions;

  @JsonIgnore(true)
  private Set<String> metrics;

  @JsonIgnore(true)
  private String jsonSchema;

  public static Schema fromFile(final File schemaFile) throws JsonParseException, JsonMappingException, IOException {
    JsonNode node = new ObjectMapper().readTree(new FileInputStream(schemaFile));
    Schema schema = new ObjectMapper().readValue(node, Schema.class);
    schema.setJSONSchema(node.toString());
    return schema;
  }

  public static Schema fromZNRecord(ZNRecord record) throws JsonParseException, JsonMappingException, IOException {
    String schemaJSON = record.getSimpleField("schemaJSON");
    Schema schema = new ObjectMapper().readValue(record.getSimpleField("schemaJSON"), Schema.class);
    schema.setJSONSchema(schemaJSON);
    return schema;
  }

  public static ZNRecord toZNRecord(Schema schema) throws IllegalArgumentException, IllegalAccessException {
    ZNRecord record = new ZNRecord(schema.getSchemaName());
    record.setSimpleField("schemaJSON", schema.getJSONSchema());
    return record;
  }

  public Schema() {
    dimensions = new HashSet<String>();
    metrics = new HashSet<String>();
    dimensionFieldSpecs = new ArrayList<DimensionFieldSpec>();
    metricFieldSpecs = new ArrayList<MetricFieldSpec>();
    timeFieldSpec = null;
    starTreeIndexSpecs = new ArrayList<StarTreeIndexSpec>();
  }

  public List<MetricFieldSpec> getMetricFieldSpecs() {
    return metricFieldSpecs;
  }

  public void setMetricFieldSpecs(List<MetricFieldSpec> metricFieldSpecs) {
    for (MetricFieldSpec spec : metricFieldSpecs) {
      addSchema(spec.getName(), spec);
    }
  }

  public List<DimensionFieldSpec> getDimensionFieldSpecs() {
    return dimensionFieldSpecs;
  }

  public void setDimensionFieldSpecs(List<DimensionFieldSpec> dimensionFieldSpecs) {
    for (DimensionFieldSpec spec : dimensionFieldSpecs) {
      addSchema(spec.getName(), spec);
    }
  }

  public void setTimeFieldSpec(TimeFieldSpec timeFieldSpec) {
    this.timeFieldSpec = timeFieldSpec;
  }

  public List<StarTreeIndexSpec> getStarTreeIndexSpecs() {
    return starTreeIndexSpecs;
  }

  public void setStarTreeIndexSpecs(List<StarTreeIndexSpec> starTreeIndexSpecs) {
    this.starTreeIndexSpecs = starTreeIndexSpecs;
  }

  @JsonIgnore(true)
  public void setJSONSchema(String schemaJSON) {
    jsonSchema = schemaJSON;
  }

  @JsonIgnore(true)
  public String getJSONSchema() {
    return jsonSchema;
  }

  @JsonIgnore(true)
  public void addSchema(String columnName, FieldSpec fieldSpec) {
    if (fieldSpec.getName() == null) {
      fieldSpec.setName(columnName);
    }

    if (columnName != null) {
      if (fieldSpec.getFieldType() == FieldType.DIMENSION) {
        if (dimensions.add(columnName)) {
          dimensionFieldSpecs.add((DimensionFieldSpec) fieldSpec);
        }
      } else if (fieldSpec.getFieldType() == FieldType.METRIC) {
        if (metrics.add(columnName)) {
          metricFieldSpecs.add((MetricFieldSpec) fieldSpec);
        }
      } else if (fieldSpec.getFieldType() == FieldType.TIME) {
        timeFieldSpec = (TimeFieldSpec) fieldSpec;
      }
    }
  }

  @JsonIgnore(true)
  public boolean isExisted(String columnName) {
    if (dimensions.contains(columnName)) {
      return true;
    }
    if (metrics.contains(columnName)) {
      return true;
    }
    if (timeFieldSpec != null && timeFieldSpec.getName().equals(columnName)) {
      return true;
    }
    return false;
  }

  @JsonIgnore(true)
  public Collection<String> getColumnNames() {
    Set<String> ret = new HashSet<String>();
    ret.addAll(metrics);
    ret.addAll(dimensions);
    if (timeFieldSpec != null) {
      ret.add(timeFieldSpec.getName());
    }
    return ret;
  }

  @JsonIgnore(true)
  public int size() {
    return metricFieldSpecs.size() + dimensionFieldSpecs.size() + (timeFieldSpec == null ? 0 : 1);
  }

  @JsonIgnore(true)
  public FieldSpec getFieldSpecFor(String column) {
    if (dimensions.contains(column)) {
      return getDimensionSpec(column);
    }
    if (metrics.contains(column)) {
      return getMetricSpec(column);
    }
    if (timeFieldSpec != null && timeFieldSpec.getName().equals(column)) {
      return getTimeFieldSpec();
    }

    return null;
  }

  @JsonIgnore(true)
  public MetricFieldSpec getMetricSpec(final String metricName) {
    return (MetricFieldSpec) CollectionUtils.find(metricFieldSpecs, new Predicate() {
      @Override
      public boolean evaluate(Object object) {
        if (object instanceof MetricFieldSpec) {
          MetricFieldSpec spec = (MetricFieldSpec) object;
          return spec.getName().equals(metricName);
        }

        return false;
      }
    });
  }

  @JsonIgnore(true)
  public DimensionFieldSpec getDimensionSpec(final String dimensionName) {
    return (DimensionFieldSpec) CollectionUtils.find(dimensionFieldSpecs, new Predicate() {
      @Override
      public boolean evaluate(Object object) {
        if (object instanceof DimensionFieldSpec) {
          DimensionFieldSpec spec = (DimensionFieldSpec) object;
          return spec.getName().equals(dimensionName);
        }
        return false;
      }
    });
  }

  @JsonIgnore(true)
  public Collection<FieldSpec> getAllFieldSpecs() {
    List<FieldSpec> ret = new ArrayList<FieldSpec>();
    ret.addAll(metricFieldSpecs);
    ret.addAll(dimensionFieldSpecs);
    if (timeFieldSpec != null) {
      ret.add(timeFieldSpec);
    }
    return ret;
  }

  @JsonIgnore(true)
  public List<String> getDimensionNames() {
    return new ArrayList<String>(dimensions);
  }

  @JsonIgnore(true)
  public List<String> getMetricNames() {
    return new ArrayList<String>(metrics);
  }

  @JsonIgnore(true)
  public String getTimeColumnName() {
    return (timeFieldSpec != null) ? timeFieldSpec.getName() : null;
  }

  @JsonIgnore(true)
  public TimeUnit getIncomingTimeUnit() {
    return (timeFieldSpec != null && timeFieldSpec.getIncomingGranularitySpec() != null) ?
        timeFieldSpec.getIncomingGranularitySpec().getTimeType() : null;
  }

  @JsonIgnore(true)
  public TimeUnit getOutgoingTimeUnit() {
    return (timeFieldSpec != null && timeFieldSpec.getOutgoingGranularitySpec() != null) ?
        timeFieldSpec.getIncomingGranularitySpec().getTimeType() : null;
  }

  public TimeFieldSpec getTimeFieldSpec() {
    return timeFieldSpec;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  @Override
  public String toString() {
    JSONObject ret = new JSONObject();
    try {
      ret.put("metricFieldSpecs", new ObjectMapper().writeValueAsString(metricFieldSpecs));
      ret.put("dimensionFieldSpecs", new ObjectMapper().writeValueAsString(dimensionFieldSpecs));
      if (timeFieldSpec != null) {
        JSONObject time = new JSONObject();
        time.put("incomingGranularitySpec",
            new ObjectMapper().writeValueAsString(timeFieldSpec.getIncomingGranularitySpec()));
        time.put("outgoingGranularitySpec",
            new ObjectMapper().writeValueAsString(timeFieldSpec.getOutgoingGranularitySpec()));
        ret.put("timeFieldSpec", time);
      } else {
        ret.put("timeFieldSpec", new JSONObject());
      }
      ret.put("schemaName", schemaName);
    } catch (Exception e) {
      LOGGER.error("error processing toString on Schema : ", this.schemaName, e);
      return null;
    }
    return ret.toString();
  }

  public static class SchemaBuilder {
    private Schema schema;

    public SchemaBuilder() {
      schema = new Schema();
    }

    public SchemaBuilder setSchemaName(String schemaName) {
      schema.setSchemaName(schemaName);
      return this;
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

  @Override
  public boolean equals(Object o) {
    if (isSameReference(this, o)) {
      return true;
    }

    if (isNullOrNotSameClass(this, o)) {
      return false;
    }

    Schema other = (Schema) o;

    return isEqual(dimensions, other.dimensions) && isEqual(timeFieldSpec, other.timeFieldSpec)
        && isEqual(metrics, other.metrics) && isEqual(schemaName, other.schemaName)
        && isEqual(starTreeIndexSpecs, other.starTreeIndexSpecs)
        && isEqual(metricFieldSpecs, other.metricFieldSpecs) && isEqual(dimensionFieldSpecs, other.dimensionFieldSpecs);
  }

  @Override
  public int hashCode() {
    int result = hashCodeOf(dimensionFieldSpecs);
    result = hashCodeOf(result, metricFieldSpecs);
    result = hashCodeOf(result, starTreeIndexSpecs);
    result = hashCodeOf(result, timeFieldSpec);
    result = hashCodeOf(result, dimensions);
    result = hashCodeOf(result, metrics);
    result = hashCodeOf(result, schemaName);
    return result;
  }
}
