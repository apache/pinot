package com.linkedin.pinot.core.indexsegment.columnar;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.joda.time.Duration;
import org.joda.time.Interval;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.indexsegment.IndexType;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;


public class ColumnarSegmentMetadata extends PropertiesConfiguration implements SegmentMetadata {

  private Map<String, FieldType> _columnsWithFieldTypeMap;
  private Map<String, DataType> _columnWithDataTypeMap;
  private Schema _segmentDataSchema;
  private String _indexDir;
  private String _segmentName;

  public ColumnarSegmentMetadata(File file) throws ConfigurationException {
    super(file);
    _indexDir = file.getAbsolutePath();
    _segmentName = file.getName();
    _columnsWithFieldTypeMap = new HashMap<String, FieldType>();
    _columnWithDataTypeMap = new HashMap<String, DataType>();
    init();

  }

  public Set<String> getAllColumnNames() {
    return _columnsWithFieldTypeMap.keySet();
  }

  public FieldType getFieldTypeFor(String column) {
    return _segmentDataSchema.getFieldType(column);
  }

  @SuppressWarnings("unchecked")
  public void init() {
    // intialize the field types map
    Iterator<String> metrics = getList(V1Constants.MetadataKeys.Segment.METRICS).iterator();
    while (metrics.hasNext()) {
      _columnsWithFieldTypeMap.put(metrics.next(), FieldType.metric);
    }

    Iterator<String> dimensions = getList(V1Constants.MetadataKeys.Segment.DIMENSIONS).iterator();
    while (dimensions.hasNext()) {
      _columnsWithFieldTypeMap.put(dimensions.next(), FieldType.dimension);
    }

    Iterator<String> unknowns = getList(V1Constants.MetadataKeys.Segment.UNKNOWN_COLUMNS).iterator();
    while (unknowns.hasNext()) {
      _columnsWithFieldTypeMap.put(unknowns.next(), FieldType.unknown);
    }

    if (containsKey(V1Constants.MetadataKeys.Segment.TIME_COLUMN_NAME)) {
      _columnsWithFieldTypeMap.put(getString(V1Constants.MetadataKeys.Segment.TIME_COLUMN_NAME), FieldType.time);
    }

    for (String column : _columnsWithFieldTypeMap.keySet()) {
      String dType =
          getString(V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.DATA_TYPE));
      System.out.println(column + " : " + dType);
      _columnWithDataTypeMap.put(column, DataType.valueOf(dType));
    }

    _segmentDataSchema = new Schema();

    for (String column : _columnsWithFieldTypeMap.keySet()) {
      FieldSpec spec =
          new FieldSpec(column, _columnsWithFieldTypeMap.get(column), _columnWithDataTypeMap.get(column), true);
      _segmentDataSchema.addSchema(column, spec);
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
    return _segmentDataSchema;
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
  public Duration getTimeGranularity() {
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

  @Override
  public String getIndexDir() {
    return _indexDir;
  }

  @Override
  public String getName() {
    return _segmentName;
  }

}
