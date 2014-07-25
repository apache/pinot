package com.linkedin.pinot.segments.v1.segment;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.joda.time.Interval;

import com.linkedin.pinot.index.IndexType;
import com.linkedin.pinot.index.data.FieldSpec;
import com.linkedin.pinot.index.data.Schema;
import com.linkedin.pinot.index.data.FieldSpec.DataType;
import com.linkedin.pinot.index.data.FieldSpec.FieldType;
import com.linkedin.pinot.index.segment.SegmentMetadata;
import com.linkedin.pinot.index.time.TimeGranularity;
import com.linkedin.pinot.segments.generator.SegmentVersion;
import com.linkedin.pinot.segments.v1.creator.V1Constants;


public class ColumnarSegmentMetadata extends PropertiesConfiguration implements SegmentMetadata {

  private Map<String, FieldType> columnsWithFieldTypeMap;
  private Map<String, DataType> columnWithDataTypeMap;
  private Schema segmentDataSchema;

  public ColumnarSegmentMetadata(File file) throws ConfigurationException {
    super(file);
    columnsWithFieldTypeMap = new HashMap<String, FieldType>();
    columnWithDataTypeMap = new HashMap<String, DataType>();
    init();
  }

  public Set<String> getAllColumnNames() {
    return columnsWithFieldTypeMap.keySet();
  }

  public FieldType getFieldTypeFor(String column) {
    return segmentDataSchema.getFieldType(column);
  }

  @SuppressWarnings("unchecked")
  public void init() {
    // intialize the field types map
    Iterator<String> metrics = getList(V1Constants.MetadataKeys.Segment.METRICS).iterator();
    while (metrics.hasNext()) {
      columnsWithFieldTypeMap.put(metrics.next(), FieldType.metric);
    }

    Iterator<String> dimensions = getList(V1Constants.MetadataKeys.Segment.DIMENSIONS).iterator();
    while (dimensions.hasNext()) {
      columnsWithFieldTypeMap.put(dimensions.next(), FieldType.dimension);
    }

    Iterator<String> unknowns = getList(V1Constants.MetadataKeys.Segment.UNKNOWN_COLUMNS).iterator();
    while (unknowns.hasNext()) {
      columnsWithFieldTypeMap.put(unknowns.next(), FieldType.unknown);
    }

    if (containsKey(V1Constants.MetadataKeys.Segment.TIME_COLUMN_NAME)) {
      columnsWithFieldTypeMap.put(getString(V1Constants.MetadataKeys.Segment.TIME_COLUMN_NAME), FieldType.time);
    }

    for (String column : columnsWithFieldTypeMap.keySet()) {
      String dType =
          getString(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.DATA_TYPE));
      System.out.println(column + " : " + dType);
      columnWithDataTypeMap.put(column, DataType.valueOf(dType));
    }

    segmentDataSchema = new Schema();

    for (String column : columnsWithFieldTypeMap.keySet()) {
      FieldSpec spec =
          new FieldSpec(column, columnsWithFieldTypeMap.get(column), columnWithDataTypeMap.get(column), true);
      segmentDataSchema.addSchema(column, spec);
    }
  }

  public String getResourceName() {
    return getString(V1Constants.MetadataKeys.Segment.RESOURCE_NAME);
  }

  @Override
  public int getTotalDocs() {
    return getInt(V1Constants.MetadataKeys.Segment.SEGMENT_TOTAL_DOCS);
  }

  @Override
  public String getVersion() {
    return SegmentVersion.v1.toString();
  }

  @Override
  public Schema getSchema() {
    return segmentDataSchema;
  }

  @Override
  public String getTableName() {
    return getString(V1Constants.MetadataKeys.Segment.TABLE_NAME);
  }

  @Override
  public String getIndexType() {
    return IndexType.columnar.toString();
  }

  // later
  @Override
  public TimeGranularity getTimeGranularity() {
    // TODO Auto-generated method stub
    return null;
  }

  // later
  @Override
  public Interval getTimeInterval() {
    // TODO Auto-generated method stub
    return null;
  }

  // later
  @Override
  public String getCrc() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getShardingKey() {
    return null;
  }

}
