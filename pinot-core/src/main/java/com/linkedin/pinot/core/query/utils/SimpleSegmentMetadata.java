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
package com.linkedin.pinot.core.query.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.joda.time.Duration;
import org.joda.time.Interval;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.SegmentMetadata;


public class SimpleSegmentMetadata implements SegmentMetadata {

  private static final String SEGMENT_SIZE = "segment.size";
  private static final String SEGMENT_RESOURCE_NAME = "segment.resource.name";
  private static final String SEGMENT_TABLE_NAME = "segment.table.name";

  private String _resourceName;
  private String _tableName;
  private String _indexType;
  private Duration _timeGranularity;
  private Interval _interval;
  private String _crc;
  private String _version;
  private Schema _schema;
  private String _shardingKey;
  private long _size;
  private String _segmentName;

  public SimpleSegmentMetadata(String resourceName, String tableName) {
    init(resourceName, tableName, new Schema());
  }

  public SimpleSegmentMetadata(String resourceName, String tableName, Schema schema) {
    init(resourceName, tableName, schema);
  }

  public SimpleSegmentMetadata() {
  }

  private void init(String resourceName, String tableName, Schema schema) {
    _resourceName = resourceName;
    _tableName = tableName;
    _schema = schema;
    _segmentName = "SimpleSegment-" + System.currentTimeMillis();
  }

  @Override
  public String getResourceName() {
    return _resourceName;
  }

  @Override
  public String getTableName() {
    return _tableName;
  }

  @Override
  public String getIndexType() {
    return _indexType;
  }

  @Override
  public Duration getTimeGranularity() {
    return _timeGranularity;
  }

  @Override
  public Interval getTimeInterval() {
    return _interval;
  }

  @Override
  public String getCrc() {
    return _crc;
  }

  @Override
  public String getVersion() {
    return _version;
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public String getShardingKey() {
    return _shardingKey;
  }

  public void setSize(long size) {
    _size = size;
  }

  public static SegmentMetadata load(Configuration properties) {
    final SegmentMetadata segmentMetadata = new SimpleSegmentMetadata();
    //segmentMetadata.setResourceName(properties.getString(SEGMENT_RESOURCE_NAME, "defaultResource"));
    //    /segmentMetadata.setTableName(properties.getString(SEGMENT_TABLE_NAME, "defaultTable"));
    ((SimpleSegmentMetadata) segmentMetadata).setSize(properties.getLong(SEGMENT_SIZE, 0));
    return segmentMetadata;
  }

  @Override
  public int getTotalDocs() {
    return (int) _size;
  }

  @Override
  public String getIndexDir() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getName() {
    return _segmentName;
  }

  @Override
  public Map<String, String> toMap() {
    // TODO Auto-generated method stub
    return new HashMap<String, String>();
  }

  @Override
  public long getIndexCreationTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getPushTime() {
    return Long.MIN_VALUE;
  }

  @Override
  public long getRefreshTime() {
    return Long.MIN_VALUE;
  }

  @Override
  public boolean hasDictionary(String columnName) {
    return false;
  }
}
