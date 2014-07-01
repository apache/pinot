package com.linkedin.pinot.query.utils;

import org.apache.commons.configuration.Configuration;
import org.joda.time.Interval;

import com.linkedin.pinot.index.data.Schema;
import com.linkedin.pinot.index.segment.SegmentMetadata;
import com.linkedin.pinot.index.time.TimeGranularity;


public class SimpleSegmentMetadata implements SegmentMetadata {

  private static final String SEGMENT_SIZE = "segment.size";
  private static final String SEGMENT_RESOURCE_NAME = "segment.resource.name";
  private static final String SEGMENT_TABLE_NAME = "segment.table.name";

  private String _resourceName;
  private String _tableName;
  private String _indexType;
  private TimeGranularity _timeGranularity;
  private Interval _interval;
  private String _crc;
  private String _version;
  private Schema _schema;
  private String _shardingKey;
  private long _size;

  @Override
  public String getResourceName() {
    return _resourceName;
  }

  @Override
  public void setResourceName(String resourceName) {
    _resourceName = resourceName;

  }

  @Override
  public String getTableName() {
    return _tableName;
  }

  @Override
  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  @Override
  public String getIndexType() {
    return _indexType;
  }

  @Override
  public TimeGranularity getTimeGranularity() {
    return _timeGranularity;
  }

  @Override
  public void setTimeGranularity(TimeGranularity timeGranularity) {
    _timeGranularity = timeGranularity;
  }

  @Override
  public Interval getTimeInterval() {
    return _interval;
  }

  @Override
  public void setTimeInterval(Interval timeInterval) {
    _interval = timeInterval;
  }

  @Override
  public String getCrc() {
    return _crc;
  }

  @Override
  public void setCrc(String crc) {
    _crc = crc;
  }

  @Override
  public String getVersion() {
    return _version;
  }

  @Override
  public void setVersion(String version) {
    _version = version;
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public void setSchema(Schema schema) {
    _schema = schema;
  }

  @Override
  public String getShardingKey() {
    return _shardingKey;
  }

  @Override
  public void setShardingKey(String shardingKey) {
    _shardingKey = shardingKey;
  }

  public void setSize(long size) {
    _size = size;
  }

  public long getSize() {
    return _size;
  }

  public static SegmentMetadata load(Configuration properties) {
    SegmentMetadata segmentMetadata = new SimpleSegmentMetadata();
    segmentMetadata.setResourceName(properties.getString(SEGMENT_RESOURCE_NAME, "defaultResource"));
    segmentMetadata.setTableName(properties.getString(SEGMENT_TABLE_NAME, "defaultTable"));
    ((SimpleSegmentMetadata) segmentMetadata).setSize(properties.getLong(SEGMENT_SIZE, 0));
    return segmentMetadata;
  }

}
