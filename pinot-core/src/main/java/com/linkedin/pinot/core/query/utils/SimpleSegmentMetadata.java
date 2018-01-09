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
package com.linkedin.pinot.core.query.utils;

import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.StarTreeMetadata;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.startree.hll.HllConstants;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.joda.time.Duration;
import org.joda.time.Interval;


public class SimpleSegmentMetadata implements SegmentMetadata {

  private static final String SEGMENT_SIZE = "segment.size";

  private String _resourceName;
  private String _indexType;
  private Duration _timeGranularity;
  private Interval _interval;
  private String _crc;
  private String _version;
  private Schema _schema;
  private String _shardingKey;
  private long _size;
  private String _segmentName;
  private char _paddingCharacter = V1Constants.Str.DEFAULT_STRING_PAD_CHAR;

  public SimpleSegmentMetadata(String resourceName) {
    init(resourceName, new Schema());
  }

  public SimpleSegmentMetadata(String resourceName, Schema schema) {
    init(resourceName, schema);
  }

  public SimpleSegmentMetadata() {
  }

  private void init(String resourceName, Schema schema) {
    _resourceName = resourceName;
    _schema = schema;

    // Added thread name so that concurrent calls to this method generate unique names.
    // Assumes multiple calls from same thread do not generate same _crc.
    _crc = System.currentTimeMillis() + "";
    _segmentName = "SimpleSegment-" + _crc + "-" + Thread.currentThread().getName();
  }

  @Override
  public String getTableName() {
    return _resourceName;
  }

  @Override
  public String getIndexType() {
    return _indexType;
  }

  @Override
  public String getTimeColumn() {
    return null;
  }

  @Override
  public long getStartTime() {
    return Long.MAX_VALUE;
  }

  @Override
  public long getEndTime() {
    return Long.MIN_VALUE;
  }

  @Override
  public TimeUnit getTimeUnit() {
    return null;
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
    ((SimpleSegmentMetadata) segmentMetadata).setSize(properties.getLong(SEGMENT_SIZE, 0));
    return segmentMetadata;
  }

  @Override
  public int getTotalDocs() {
    return (int) _size;
  }

  @Override
  public int getTotalRawDocs() {
    return (int) _size;
  }

  @Override
  public String getIndexDir() {
    return null;
  }

  @Override
  public String getName() {
    return _segmentName;
  }

  @Override
  public Map<String, String> toMap() {
    return new HashMap<String, String>();
  }

  @Override
  public long getIndexCreationTime() {
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

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public boolean hasStarTree() {
    return false;
  }

  @Nullable
  @Override
  public StarTreeMetadata getStarTreeMetadata() {
    return null;
  }

  @Override
  public String getForwardIndexFileName(String column) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getDictionaryFileName(String column) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getBitmapInvertedIndexFileName(String column) {
    // TODO Auto-generated method stub
    return null;
  }

  @Nullable
  @Override
  public String getCreatorName() {
    return null;
  }

  @Override
  public char getPaddingCharacter() {
    return _paddingCharacter;
  }

  @Override
  public int getHllLog2m() {
    return HllConstants.DEFAULT_LOG2M;
  }

  @Nullable
  @Override
  public String getDerivedColumn(String column, MetricFieldSpec.DerivedMetricType derivedMetricType) {
    return null;
  }
}
