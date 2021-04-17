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
package org.apache.pinot.common.metadata.segment;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadata;
import org.apache.pinot.spi.utils.CommonConstants.Segment;
import org.apache.pinot.spi.utils.CommonConstants.Segment.SegmentType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class SegmentZKMetadata implements ZKMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentZKMetadata.class);

  protected static final String NULL = "null";

  private String _segmentName;
  private SegmentType _segmentType;
  private long _startTime = -1;
  private long _endTime = -1;
  private TimeUnit _timeUnit;
  private String _indexVersion;
  private long _totalDocs = -1;
  private long _crc = -1;
  private long _creationTime = -1;
  private SegmentPartitionMetadata _partitionMetadata;
  private long _segmentUploadStartTime = -1;
  private String _crypterName;
  private Map<String, String> _customMap;

  @Deprecated
  private String _rawTableName;

  public SegmentZKMetadata() {
  }

  public SegmentZKMetadata(ZNRecord znRecord) {
    _segmentName = znRecord.getSimpleField(Segment.SEGMENT_NAME);
    _segmentType = znRecord.getEnumField(Segment.SEGMENT_TYPE, SegmentType.class, SegmentType.OFFLINE);
    _startTime = znRecord.getLongField(Segment.START_TIME, -1);
    _endTime = znRecord.getLongField(Segment.END_TIME, -1);
    String timeUnitString = znRecord.getSimpleField(Segment.TIME_UNIT);
    if (timeUnitString != null && !timeUnitString.equals(NULL)) {
      _timeUnit = znRecord.getEnumField(Segment.TIME_UNIT, TimeUnit.class, TimeUnit.DAYS);
    }
    _indexVersion = znRecord.getSimpleField(Segment.INDEX_VERSION);
    _totalDocs = znRecord.getLongField(Segment.TOTAL_DOCS, -1);
    _crc = znRecord.getLongField(Segment.CRC, -1);
    _creationTime = znRecord.getLongField(Segment.CREATION_TIME, -1);
    try {
      String partitionMetadataJson = znRecord.getSimpleField(Segment.PARTITION_METADATA);
      if (partitionMetadataJson != null) {
        _partitionMetadata = SegmentPartitionMetadata.fromJsonString(partitionMetadataJson);
      }
    } catch (IOException e) {
      LOGGER.error(
          "Exception caught while reading partition info from zk metadata for segment '{}', partition info dropped.",
          _segmentName, e);
    }
    _segmentUploadStartTime = znRecord.getLongField(Segment.SEGMENT_UPLOAD_START_TIME, -1);
    _crypterName = znRecord.getSimpleField(Segment.CRYPTER_NAME);
    _customMap = znRecord.getMapField(Segment.CUSTOM_MAP);

    // For backward-compatibility
    setTableName(znRecord.getSimpleField(Segment.TABLE_NAME));
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public void setSegmentName(String segmentName) {
    _segmentName = segmentName;
  }

  public SegmentType getSegmentType() {
    return _segmentType;
  }

  public void setSegmentType(SegmentType segmentType) {
    _segmentType = segmentType;
  }

  public long getStartTimeMs() {
    if (_startTime > 0 && _timeUnit != null) {
      return _timeUnit.toMillis(_startTime);
    } else {
      return -1;
    }
  }

  public void setStartTime(long startTime) {
    _startTime = startTime;
  }

  public long getEndTimeMs() {
    if (_endTime > 0 && _timeUnit != null) {
      return _timeUnit.toMillis(_endTime);
    } else {
      return -1;
    }
  }

  public void setEndTime(long endTime) {
    _endTime = endTime;
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

  public String getCrypterName() {
    return _crypterName;
  }

  public void setCrypterName(String crypterName) {
    _crypterName = crypterName;
  }

  public Map<String, String> getCustomMap() {
    return _customMap;
  }

  public void setCustomMap(Map<String, String> customMap) {
    _customMap = customMap;
  }

  @Deprecated
  public String getTableName() {
    return _rawTableName;
  }

  @Deprecated
  public void setTableName(String tableName) {
    _rawTableName = tableName != null ? TableNameBuilder.extractRawTableName(tableName) : null;
  }

  @Deprecated
  public long getStartTime() {
    return _startTime;
  }

  @Deprecated
  public long getEndTime() {
    return _endTime;
  }

  @Deprecated
  public TimeUnit getTimeUnit() {
    return _timeUnit;
  }

  @Deprecated
  public Duration getTimeGranularity() {
    return _timeUnit != null ? new Duration(_timeUnit.toMillis(1)) : null;
  }

  @Deprecated
  public Interval getTimeInterval() {
    if (_startTime > 0 && _startTime <= _endTime && _timeUnit != null) {
      return new Interval(_timeUnit.toMillis(_startTime), _timeUnit.toMillis(_endTime));
    } else {
      return null;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentZKMetadata that = (SegmentZKMetadata) o;
    return _startTime == that._startTime && _endTime == that._endTime && _totalDocs == that._totalDocs
        && _crc == that._crc && _creationTime == that._creationTime
        && _segmentUploadStartTime == that._segmentUploadStartTime && Objects.equals(_segmentName, that._segmentName)
        && _segmentType == that._segmentType && _timeUnit == that._timeUnit
        && Objects.equals(_indexVersion, that._indexVersion)
        && Objects.equals(_partitionMetadata, that._partitionMetadata)
        && Objects.equals(_crypterName, that._crypterName) && Objects.equals(_customMap, that._customMap)
        && Objects.equals(_rawTableName, that._rawTableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_segmentName, _segmentType, _startTime, _endTime, _timeUnit, _indexVersion, _totalDocs, _crc,
        _creationTime, _partitionMetadata, _segmentUploadStartTime, _crypterName, _customMap, _rawTableName);
  }

  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_segmentName);

    znRecord.setSimpleField(Segment.SEGMENT_NAME, _segmentName);
    znRecord.setEnumField(Segment.SEGMENT_TYPE, _segmentType);
    znRecord.setLongField(Segment.START_TIME, _startTime);
    znRecord.setLongField(Segment.END_TIME, _endTime);
    znRecord.setSimpleField(Segment.TIME_UNIT, _timeUnit != null ? _timeUnit.name() : NULL);
    znRecord.setSimpleField(Segment.INDEX_VERSION, _indexVersion);
    znRecord.setLongField(Segment.TOTAL_DOCS, _totalDocs);
    znRecord.setLongField(Segment.CRC, _crc);
    znRecord.setLongField(Segment.CREATION_TIME, _creationTime);

    if (_partitionMetadata != null) {
      try {
        String partitionMetadataJson = _partitionMetadata.toJsonString();
        znRecord.setSimpleField(Segment.PARTITION_METADATA, partitionMetadataJson);
      } catch (IOException e) {
        LOGGER.error(
            "Exception caught while writing partition metadata into ZNRecord for segment '{}', will be dropped",
            _segmentName, e);
      }
    }
    if (_segmentUploadStartTime > 0) {
      znRecord.setLongField(Segment.SEGMENT_UPLOAD_START_TIME, _segmentUploadStartTime);
    }
    if (_crypterName != null) {
      znRecord.setSimpleField(Segment.CRYPTER_NAME, _crypterName);
    }
    if (_customMap != null) {
      znRecord.setMapField(Segment.CUSTOM_MAP, _customMap);
    }

    // For backward-compatibility
    if (_rawTableName != null) {
      znRecord.setSimpleField(Segment.TABLE_NAME, _rawTableName);
    }

    return znRecord;
  }

  public Map<String, String> toMap() {
    Map<String, String> configMap = new HashMap<>();

    configMap.put(Segment.SEGMENT_NAME, _segmentName);
    configMap.put(Segment.SEGMENT_TYPE, _segmentType.toString());
    configMap.put(Segment.START_TIME, Long.toString(_startTime));
    configMap.put(Segment.END_TIME, Long.toString(_endTime));
    configMap.put(Segment.TIME_UNIT, _timeUnit != null ? _timeUnit.name() : null);
    configMap.put(Segment.INDEX_VERSION, _indexVersion);
    configMap.put(Segment.TOTAL_DOCS, Long.toString(_totalDocs));
    configMap.put(Segment.CRC, Long.toString(_crc));
    configMap.put(Segment.CREATION_TIME, Long.toString(_creationTime));

    if (_partitionMetadata != null) {
      try {
        String partitionMetadataJson = _partitionMetadata.toJsonString();
        configMap.put(Segment.PARTITION_METADATA, partitionMetadataJson);
      } catch (IOException e) {
        LOGGER.error(
            "Exception caught while converting partition metadata into JSON string for segment '{}', will be dropped",
            _segmentName, e);
      }
    }
    if (_segmentUploadStartTime > 0) {
      configMap.put(Segment.SEGMENT_UPLOAD_START_TIME, Long.toString(_segmentUploadStartTime));
    }
    if (_crypterName != null) {
      configMap.put(Segment.CRYPTER_NAME, _crypterName);
    }
    if (_customMap != null) {
      try {
        configMap.put(Segment.CUSTOM_MAP, JsonUtils.objectToString(_customMap));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    // For backward-compatibility
    if (_rawTableName != null) {
      configMap.put(Segment.TABLE_NAME, _rawTableName);
    }

    return configMap;
  }
}
