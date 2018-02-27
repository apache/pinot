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
package com.linkedin.pinot.common.metadata.segment;

import com.linkedin.pinot.common.metadata.ZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.helix.ZNRecord;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.pinot.common.utils.EqualityUtils.*;


public abstract class SegmentZKMetadata implements ZKMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentZKMetadata.class);

  private static final String NULL = "null";

  private String _segmentName;
  private String _tableName;
  private SegmentType _segmentType;
  private long _startTime = -1;
  private long _endTime = -1;
  private TimeUnit _timeUnit;
  private Duration _timeGranularity;
  private Interval _timeInterval;
  private String _indexVersion;
  private long _totalRawDocs = -1;
  private long _crc = -1;
  private long _creationTime = -1;
  private SegmentPartitionMetadata _partitionMetadata;
  private long _segmentUploadStartTime = -1;
  private Map<String, String> _customMap;

  public SegmentZKMetadata() {
  }

  public SegmentZKMetadata(ZNRecord znRecord) {
    _segmentName = znRecord.getSimpleField(CommonConstants.Segment.SEGMENT_NAME);
    _tableName = znRecord.getSimpleField(CommonConstants.Segment.TABLE_NAME);
    _segmentType = znRecord.getEnumField(CommonConstants.Segment.SEGMENT_TYPE, SegmentType.class, SegmentType.OFFLINE);
    _startTime = znRecord.getLongField(CommonConstants.Segment.START_TIME, -1);
    _endTime = znRecord.getLongField(CommonConstants.Segment.END_TIME, -1);
    if (znRecord.getSimpleFields().containsKey(CommonConstants.Segment.TIME_UNIT) && !znRecord.getSimpleField(
        CommonConstants.Segment.TIME_UNIT).equals(NULL)) {
      setTimeUnit(znRecord.getEnumField(CommonConstants.Segment.TIME_UNIT, TimeUnit.class, TimeUnit.DAYS));
    }
    _indexVersion = znRecord.getSimpleField(CommonConstants.Segment.INDEX_VERSION);
    _totalRawDocs = znRecord.getLongField(CommonConstants.Segment.TOTAL_DOCS, -1);
    _crc = znRecord.getLongField(CommonConstants.Segment.CRC, -1);
    _creationTime = znRecord.getLongField(CommonConstants.Segment.CREATION_TIME, -1);

    try {
      String partitionMetadataJson = znRecord.getSimpleField(CommonConstants.Segment.PARTITION_METADATA);
      if (partitionMetadataJson != null) {
        _partitionMetadata = SegmentPartitionMetadata.fromJsonString(partitionMetadataJson);
      }
    } catch (IOException e) {
      LOGGER.error(
          "Exception caught while reading partition info from zk metadata for segment '{}', partition info dropped.",
          _segmentName, e);
    }
    _segmentUploadStartTime = znRecord.getLongField(CommonConstants.Segment.SEGMENT_UPLOAD_START_TIME, -1);
    _customMap = znRecord.getMapField(CommonConstants.Segment.CUSTOM_MAP);
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public void setSegmentName(String segmentName) {
    _segmentName = segmentName;
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

  /**
   * NOTE: should be called after setting start and end time.
   */
  public void setTimeUnit(@Nonnull TimeUnit timeUnit) {
    _timeUnit = timeUnit;
    _timeGranularity = new Duration(_timeUnit.toMillis(1));
    // For consuming segment, end time might not be set
    if (_startTime >= 0 && _startTime <= _endTime) {
      _timeInterval = new Interval(_timeUnit.toMillis(_startTime), _timeUnit.toMillis(_endTime));
    }
  }

  public Duration getTimeGranularity() {
    return _timeGranularity;
  }

  public Interval getTimeInterval() {
    return _timeInterval;
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

  public long getTotalRawDocs() {
    return _totalRawDocs;
  }

  public void setTotalRawDocs(long totalRawDocs) {
    _totalRawDocs = totalRawDocs;
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

  public void setPartitionMetadata(SegmentPartitionMetadata partitionMetadata) {
    _partitionMetadata = partitionMetadata;
  }

  public SegmentPartitionMetadata getPartitionMetadata() {
    return _partitionMetadata;
  }

  public long getSegmentUploadStartTime() {
    return _segmentUploadStartTime;
  }

  public void setSegmentUploadStartTime(long segmentUploadStartTime) {
    _segmentUploadStartTime = segmentUploadStartTime;
  }

  public Map<String, String> getCustomMap() {
    return _customMap;
  }

  public void setCustomMap(Map<String, String> customMap) {
    _customMap = customMap;
  }

  @Override
  public boolean equals(Object segmentMetadata) {
    if (isSameReference(this, segmentMetadata)) {
      return true;
    }

    if (isNullOrNotSameClass(this, segmentMetadata)) {
      return false;
    }

    SegmentZKMetadata metadata = (SegmentZKMetadata) segmentMetadata;
    return isEqual(_segmentName, metadata._segmentName) && isEqual(_tableName, metadata._tableName) && isEqual(
        _indexVersion, metadata._indexVersion) && isEqual(_timeUnit, metadata._timeUnit) && isEqual(_startTime,
        metadata._startTime) && isEqual(_endTime, metadata._endTime) && isEqual(_segmentType, metadata._segmentType)
        && isEqual(_totalRawDocs, metadata._totalRawDocs) && isEqual(_crc, metadata._crc) && isEqual(_creationTime,
        metadata._creationTime) && isEqual(_partitionMetadata, metadata._partitionMetadata) && isEqual(
        _segmentUploadStartTime, metadata._segmentUploadStartTime) && isEqual(_customMap, metadata._customMap);
  }

  @Override
  public int hashCode() {
    int result = hashCodeOf(_segmentName);
    result = hashCodeOf(result, _tableName);
    result = hashCodeOf(result, _segmentType);
    result = hashCodeOf(result, _startTime);
    result = hashCodeOf(result, _endTime);
    result = hashCodeOf(result, _timeUnit);
    result = hashCodeOf(result, _indexVersion);
    result = hashCodeOf(result, _totalRawDocs);
    result = hashCodeOf(result, _crc);
    result = hashCodeOf(result, _creationTime);
    result = hashCodeOf(result, _partitionMetadata);
    result = hashCodeOf(result, _segmentUploadStartTime);
    result = hashCodeOf(result, _customMap);
    return result;
  }

  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_segmentName);
    znRecord.setSimpleField(CommonConstants.Segment.SEGMENT_NAME, _segmentName);
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
    znRecord.setLongField(CommonConstants.Segment.TOTAL_DOCS, _totalRawDocs);
    znRecord.setLongField(CommonConstants.Segment.CRC, _crc);
    znRecord.setLongField(CommonConstants.Segment.CREATION_TIME, _creationTime);

    if (_partitionMetadata != null) {
      try {
        String partitionMetadataJson = _partitionMetadata.toJsonString();
        znRecord.setSimpleField(CommonConstants.Segment.PARTITION_METADATA, partitionMetadataJson);
      } catch (IOException e) {
        LOGGER.error(
            "Exception caught while writing partition metadata into ZNRecord for segment '{}', will be dropped",
            _segmentName, e);
      }
    }
    if (_segmentUploadStartTime > 0) {
      znRecord.setLongField(CommonConstants.Segment.SEGMENT_UPLOAD_START_TIME, _segmentUploadStartTime);
    }
    if (_customMap != null) {
      znRecord.setMapField(CommonConstants.Segment.CUSTOM_MAP, _customMap);
    }
    return znRecord;
  }

  public Map<String, String> toMap() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(CommonConstants.Segment.SEGMENT_NAME, _segmentName);
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
    configMap.put(CommonConstants.Segment.TOTAL_DOCS, Long.toString(_totalRawDocs));
    configMap.put(CommonConstants.Segment.CRC, Long.toString(_crc));
    configMap.put(CommonConstants.Segment.CREATION_TIME, Long.toString(_creationTime));

    if (_partitionMetadata != null) {
      try {
        String partitionMetadataJson = _partitionMetadata.toJsonString();
        configMap.put(CommonConstants.Segment.PARTITION_METADATA, partitionMetadataJson);
      } catch (IOException e) {
        LOGGER.error(
            "Exception caught while converting partition metadata into JSON string for segment '{}', will be dropped",
            _segmentName, e);
      }
    }

    if (_segmentUploadStartTime > 0) {
      configMap.put(CommonConstants.Segment.SEGMENT_UPLOAD_START_TIME, Long.toString(_segmentUploadStartTime));
    }

    if (_customMap == null) {
      configMap.put(CommonConstants.Segment.CUSTOM_MAP, null);
    } else {
      JSONObject jsonObject = new JSONObject(_customMap);
      configMap.put(CommonConstants.Segment.CUSTOM_MAP, jsonObject.toString());
    }

    return configMap;
  }
}
