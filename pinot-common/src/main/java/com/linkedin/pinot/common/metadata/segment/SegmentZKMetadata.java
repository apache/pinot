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
package com.linkedin.pinot.common.metadata.segment;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.helix.ZNRecord;

import com.linkedin.pinot.common.metadata.ZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;


public abstract class SegmentZKMetadata implements ZKMetadata {

  private static final String NULL = "null";

  private String _segmentName = null;
  private String _resourceName = null;
  private String _tableName = null;
  private SegmentType _segmentType = null;
  private long _startTime = -1;
  private long _endTime = -1;
  private TimeUnit _timeUnit = null;
  private String _indexVersion = null;
  private long _totalDocs = -1;
  private long _crc = -1;
  private long _creationTime = -1;

  public SegmentZKMetadata() {
  }

  public SegmentZKMetadata(ZNRecord znRecord) {
    _segmentName = znRecord.getSimpleField(CommonConstants.Segment.SEGMENT_NAME);
    _resourceName = znRecord.getSimpleField(CommonConstants.Segment.RESOURCE_NAME);
    _tableName = znRecord.getSimpleField(CommonConstants.Segment.TABLE_NAME);
    _segmentType = znRecord.getEnumField(CommonConstants.Segment.SEGMENT_TYPE, SegmentType.class, SegmentType.OFFLINE);
    _startTime = znRecord.getLongField(CommonConstants.Segment.START_TIME, -1);
    _endTime = znRecord.getLongField(CommonConstants.Segment.END_TIME, -1);
    if (znRecord.getSimpleFields().containsKey(CommonConstants.Segment.TIME_UNIT) &&
        !znRecord.getSimpleField(CommonConstants.Segment.TIME_UNIT).equals(NULL)) {
      _timeUnit = znRecord.getEnumField(CommonConstants.Segment.TIME_UNIT, TimeUnit.class, TimeUnit.DAYS);
    }
    _indexVersion = znRecord.getSimpleField(CommonConstants.Segment.INDEX_VERSION);
    _totalDocs = znRecord.getLongField(CommonConstants.Segment.TOTAL_DOCS, -1);
    _crc = znRecord.getLongField(CommonConstants.Segment.CRC, -1);
    _creationTime = znRecord.getLongField(CommonConstants.Segment.CREATION_TIME, -1);
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public void setSegmentName(String segmentName) {
    _segmentName = segmentName;
  }

  public String getResourceName() {
    return _resourceName;
  }

  public void setResourceName(String resourceName) {
    _resourceName = resourceName;
  }

  public String getTableName() {
    return _tableName;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  public long getStartTime() {
    return _startTime;
  }

  public void setStartTime(long startTime) {
    _startTime = startTime;
  }

  public long getEndTime() {
    return _endTime;
  }

  public void setEndTime(long endTime) {
    _endTime = endTime;
  }

  public TimeUnit getTimeUnit() {
    return _timeUnit;
  }

  public void setTimeUnit(TimeUnit timeUnit) {
    _timeUnit = timeUnit;
  }

  public String getIndexVersion() {
    return _indexVersion;
  }

  public void setIndexVersion(String indexVersion) {
    _indexVersion = indexVersion;
  }

  public SegmentType getSegmentType() {
    return _segmentType;
  }

  public void setSegmentType(SegmentType segmentType) {
    _segmentType = segmentType;
  }

  public long getTotalDocs() {
    return _totalDocs;
  }

  public void setTotalDocs(long totalDocs) {
    _totalDocs = totalDocs;
  }

  public long getCrc() {
    return _crc;
  }

  public void setCrc(long crc) {
    _crc = crc;
  }

  public long getCreationTime() {
    return _creationTime;
  }

  public void setCreationTime(long creationTime) {
    _creationTime = creationTime;
  }

  @Override
  public boolean equals(Object segmentMetadata) {
    if (!(segmentMetadata instanceof SegmentZKMetadata)) {
      return false;
    }
    SegmentZKMetadata metadata = (SegmentZKMetadata) segmentMetadata;
    if (!getSegmentName().equals(metadata.getSegmentName()) ||
        !getResourceName().equals(metadata.getResourceName()) ||
        !getTableName().equals(metadata.getTableName()) ||
        !getIndexVersion().equals(metadata.getIndexVersion()) ||
        getTimeUnit() != metadata.getTimeUnit() ||
        getStartTime() != metadata.getStartTime() ||
        getEndTime() != metadata.getEndTime() ||
        getSegmentType() != metadata.getSegmentType() ||
        getTotalDocs() != metadata.getTotalDocs() ||
        getCrc() != metadata.getCrc() ||
        getCreationTime() != metadata.getCreationTime()) {
      return false;
    }
    return true;
  }

  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_segmentName);
    znRecord.setSimpleField(CommonConstants.Segment.SEGMENT_NAME, _segmentName);
    znRecord.setSimpleField(CommonConstants.Segment.RESOURCE_NAME, _resourceName);
    znRecord.setSimpleField(CommonConstants.Segment.TABLE_NAME, _tableName);
    znRecord.setEnumField(CommonConstants.Segment.SEGMENT_TYPE, _segmentType);
    if (_timeUnit == null) {
      znRecord.setSimpleField(CommonConstants.Segment.TIME_UNIT, NULL);
    } else {
      znRecord.setEnumField(CommonConstants.Segment.TIME_UNIT, _timeUnit);
    }
    znRecord.setLongField(CommonConstants.Segment.START_TIME, _startTime);
    znRecord.setLongField(CommonConstants.Segment.END_TIME, _endTime);

    znRecord.setSimpleField(CommonConstants.Segment.INDEX_VERSION, _indexVersion);
    znRecord.setLongField(CommonConstants.Segment.TOTAL_DOCS, _totalDocs);
    znRecord.setLongField(CommonConstants.Segment.CRC, _crc);
    znRecord.setLongField(CommonConstants.Segment.CREATION_TIME, _creationTime);
    return znRecord;
  }

  public Map<String, String> toMap() {
    Map<String, String> configMap = new HashMap<String, String>();
    configMap.put(CommonConstants.Segment.SEGMENT_NAME, _segmentName);
    configMap.put(CommonConstants.Segment.RESOURCE_NAME, _resourceName);
    configMap.put(CommonConstants.Segment.TABLE_NAME, _tableName);
    configMap.put(CommonConstants.Segment.SEGMENT_TYPE, _segmentType.toString());
    if (_timeUnit == null) {
      configMap.put(CommonConstants.Segment.TIME_UNIT, null);
    } else {
      configMap.put(CommonConstants.Segment.TIME_UNIT, _timeUnit.toString());
    }
    configMap.put(CommonConstants.Segment.START_TIME, Long.toString(_startTime));
    configMap.put(CommonConstants.Segment.END_TIME, Long.toString(_endTime));

    configMap.put(CommonConstants.Segment.INDEX_VERSION, _indexVersion);
    configMap.put(CommonConstants.Segment.TOTAL_DOCS, Long.toString(_totalDocs));
    configMap.put(CommonConstants.Segment.CRC, Long.toString(_crc));
    configMap.put(CommonConstants.Segment.CREATION_TIME, Long.toString(_creationTime));
    return configMap;
  }
}
