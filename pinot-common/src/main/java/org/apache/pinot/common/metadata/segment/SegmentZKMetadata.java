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

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.MapUtils;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadata;
import org.apache.pinot.spi.utils.CommonConstants.Segment;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentZKMetadata implements ZKMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentZKMetadata.class);
  private static final String NULL = "null";

  private final ZNRecord _znRecord;
  private Map<String, String> _simpleFields;

  // Cache start/end time because they can be used to sort the metadata
  private boolean _startTimeMsCached;
  private long _startTimeMs;
  private boolean _endTimeMsCached;
  private long _endTimeMs;

  public SegmentZKMetadata(String segmentName) {
    _znRecord = new ZNRecord(segmentName);
    _simpleFields = _znRecord.getSimpleFields();
  }

  public SegmentZKMetadata(ZNRecord znRecord) {
    _znRecord = znRecord;
    _simpleFields = znRecord.getSimpleFields();
  }

  public String getSegmentName() {
    return _znRecord.getId();
  }

  public long getStartTimeMs() {
    if (!_startTimeMsCached) {
      long startTimeMs = -1;
      String startTimeString = _simpleFields.get(Segment.START_TIME);
      if (startTimeString != null) {
        long startTime = Long.parseLong(startTimeString);
        // NOTE: Need to check whether the start time is positive because some old segment ZK metadata contains negative
        //       start time and null time unit
        if (startTime > 0) {
          startTimeMs = TimeUnit.valueOf(_simpleFields.get(Segment.TIME_UNIT)).toMillis(startTime);
        }
      }
      _startTimeMs = startTimeMs;
      _startTimeMsCached = true;
    }
    return _startTimeMs;
  }

  public long getEndTimeMs() {
    if (!_endTimeMsCached) {
      long endTimeMs = -1;
      String endTimeString = _simpleFields.get(Segment.END_TIME);
      if (endTimeString != null) {
        long endTime = Long.parseLong(endTimeString);
        // NOTE: Need to check whether the end time is positive because some old segment ZK metadata contains negative
        //       end time and null time unit
        if (endTime > 0) {
          endTimeMs = TimeUnit.valueOf(_simpleFields.get(Segment.TIME_UNIT)).toMillis(endTime);
        }
      }
      _endTimeMs = endTimeMs;
      _endTimeMsCached = true;
    }
    return _endTimeMs;
  }

  public String getRawStartTime() {
    return _simpleFields.get(Segment.RAW_START_TIME);
  }

  public String getRawEndTime() {
    return _simpleFields.get(Segment.RAW_END_TIME);
  }

  public void setStartTime(long startTime) {
    setNonNegativeValue(Segment.START_TIME, startTime);
    _startTimeMsCached = false;
  }

  public void setEndTime(long endTime) {
    setNonNegativeValue(Segment.END_TIME, endTime);
    _endTimeMsCached = false;
  }

  public void setRawStartTime(String startTime) {
    setValue(Segment.RAW_START_TIME, startTime);
  }

  public void setRawEndTime(String endTime) {
    setValue(Segment.RAW_END_TIME, endTime);
  }

  public void setTimeUnit(TimeUnit timeUnit) {
    setValue(Segment.TIME_UNIT, timeUnit);
    _startTimeMsCached = false;
    _endTimeMsCached = false;
  }

  public String getIndexVersion() {
    return _simpleFields.get(Segment.INDEX_VERSION);
  }

  public void setIndexVersion(String indexVersion) {
    setValue(Segment.INDEX_VERSION, indexVersion);
  }

  public long getTotalDocs() {
    return _znRecord.getLongField(Segment.TOTAL_DOCS, -1);
  }

  public void setTotalDocs(long totalDocs) {
    setNonNegativeValue(Segment.TOTAL_DOCS, totalDocs);
  }

  public void setSizeInBytes(long sizeInBytes) {
    setNonNegativeValue(Segment.SIZE_IN_BYTES, sizeInBytes);
  }

  public long getSizeInBytes() {
    return _znRecord.getLongField(Segment.SIZE_IN_BYTES, -1);
  }

  public long getCrc() {
    return _znRecord.getLongField(Segment.CRC, -1);
  }

  public void setCrc(long crc) {
    setNonNegativeValue(Segment.CRC, crc);
  }

  public String getTier() {
    return _simpleFields.get(Segment.TIER);
  }

  public void setTier(String tier) {
    setValue(Segment.TIER, tier);
  }

  /**
   * For uploaded segment, this is the time when the segment file is created. For real-time segment, this is the time
   * when the consuming segment is created.
   */
  public long getCreationTime() {
    return _znRecord.getLongField(Segment.CREATION_TIME, -1);
  }

  public void setCreationTime(long creationTime) {
    setNonNegativeValue(Segment.CREATION_TIME, creationTime);
  }

  /**
   * Push time exists only for uploaded segments. It is the time when the segment is first pushed to the cluster (i.e.
   * when the segment ZK metadata is created).
   */
  public long getPushTime() {
    String pushTimeString = _simpleFields.get(Segment.PUSH_TIME);
    // Handle legacy push time key
    if (pushTimeString == null) {
      pushTimeString = _simpleFields.get(Segment.Offline.PUSH_TIME);
    }
    // Return Long.MIN_VALUE if unavailable for backward compatibility
    return pushTimeString != null ? Long.parseLong(pushTimeString) : Long.MIN_VALUE;
  }

  public void setPushTime(long pushTime) {
    setNonNegativeValue(Segment.PUSH_TIME, pushTime);
  }

  /**
   * Refresh time exists only for uploaded segments that have been replaced. It is the time when the segment is last
   * replaced.
   */
  public long getRefreshTime() {
    String refreshTimeString = _simpleFields.get(Segment.REFRESH_TIME);
    // Handle legacy refresh time key
    if (refreshTimeString == null) {
      refreshTimeString = _simpleFields.get(Segment.Offline.REFRESH_TIME);
    }
    // Return Long.MIN_VALUE if unavailable for backward compatibility
    return refreshTimeString != null ? Long.parseLong(refreshTimeString) : Long.MIN_VALUE;
  }

  public void setRefreshTime(long pushTime) {
    setNonNegativeValue(Segment.REFRESH_TIME, pushTime);
  }

  public String getDownloadUrl() {
    String downloadUrl = _simpleFields.get(Segment.DOWNLOAD_URL);
    // Handle legacy download url keys
    if (downloadUrl == null) {
      downloadUrl = _simpleFields.get(Segment.Offline.DOWNLOAD_URL);
      if (downloadUrl == null) {
        downloadUrl = _simpleFields.get(Segment.Realtime.DOWNLOAD_URL);
      }
    }
    return downloadUrl;
  }

  public void setDownloadUrl(String downloadUrl) {
    setValue(Segment.DOWNLOAD_URL, downloadUrl);
  }

  public String getCrypterName() {
    return _simpleFields.get(Segment.CRYPTER_NAME);
  }

  public void setCrypterName(String crypterName) {
    setValue(Segment.CRYPTER_NAME, crypterName);
  }

  public SegmentPartitionMetadata getPartitionMetadata() {
    String partitionMetadataJson = _simpleFields.get(Segment.PARTITION_METADATA);
    if (partitionMetadataJson != null) {
      try {
        return SegmentPartitionMetadata.fromJsonString(partitionMetadataJson);
      } catch (Exception e) {
        LOGGER.error("Caught exception while reading partition metadata for segment: {}", getSegmentName(), e);
      }
    }
    return null;
  }

  public void setPartitionMetadata(SegmentPartitionMetadata partitionMetadata) {
    if (partitionMetadata != null) {
      try {
        _simpleFields.put(Segment.PARTITION_METADATA, partitionMetadata.toJsonString());
      } catch (Exception e) {
        LOGGER.error("Caught exception while writing partition metadata for segment: {}", getSegmentName(), e);
      }
    } else {
      _simpleFields.remove(Segment.PARTITION_METADATA);
    }
  }

  public Map<String, String> getCustomMap() {
    return _znRecord.getMapField(Segment.CUSTOM_MAP);
  }

  public void setCustomMap(Map<String, String> customMap) {
    Map<String, Map<String, String>> mapFields = _znRecord.getMapFields();
    if (MapUtils.isNotEmpty(customMap)) {
      mapFields.put(Segment.CUSTOM_MAP, customMap);
    } else {
      mapFields.remove(Segment.CUSTOM_MAP);
    }
  }

  /* FOR REALTIME SEGMENTS */

  public Status getStatus() {
    return _znRecord.getEnumField(Segment.Realtime.STATUS, Status.class, Status.UPLOADED);
  }

  public void setStatus(Status status) {
    setValue(Segment.Realtime.STATUS, status);
  }

  public int getSizeThresholdToFlushSegment() {
    return _znRecord.getIntField(Segment.Realtime.FLUSH_THRESHOLD_SIZE, -1);
  }

  public void setSizeThresholdToFlushSegment(int flushThresholdSize) {
    setNonNegativeValue(Segment.Realtime.FLUSH_THRESHOLD_SIZE, flushThresholdSize);
  }

  public String getTimeThresholdToFlushSegment() {
    // Check "null" for backward-compatibility
    String flushThresholdTime = _simpleFields.get(Segment.Realtime.FLUSH_THRESHOLD_TIME);
    if (flushThresholdTime != null && !flushThresholdTime.equals(NULL)) {
      return flushThresholdTime;
    } else {
      return null;
    }
  }

  public void setTimeThresholdToFlushSegment(String flushThresholdTime) {
    setValue(Segment.Realtime.FLUSH_THRESHOLD_TIME, flushThresholdTime);
  }

  public String getStartOffset() {
    return _simpleFields.get(Segment.Realtime.START_OFFSET);
  }

  public void setStartOffset(String startOffset) {
    setValue(Segment.Realtime.START_OFFSET, startOffset);
  }

  public String getEndOffset() {
    return _simpleFields.get(Segment.Realtime.END_OFFSET);
  }

  public void setEndOffset(String endOffset) {
    setValue(Segment.Realtime.END_OFFSET, endOffset);
  }

  public int getNumReplicas() {
    return _znRecord.getIntField(Segment.Realtime.NUM_REPLICAS, -1);
  }

  public void setNumReplicas(int numReplicas) {
    setNonNegativeValue(Segment.Realtime.NUM_REPLICAS, numReplicas);
  }

  /* FOR PARALLEL PUSH PROTECTION */

  public long getSegmentUploadStartTime() {
    return _znRecord.getLongField(Segment.SEGMENT_UPLOAD_START_TIME, -1);
  }

  public void setSegmentUploadStartTime(long segmentUploadStartTime) {
    setNonNegativeValue(Segment.SEGMENT_UPLOAD_START_TIME, segmentUploadStartTime);
  }

  private void setValue(String key, Object value) {
    if (value != null) {
      _simpleFields.put(key, value.toString());
    } else {
      _simpleFields.remove(key);
    }
  }

  private void setNonNegativeValue(String key, long value) {
    if (value >= 0) {
      _simpleFields.put(key, Long.toString(value));
    } else {
      _simpleFields.remove(key);
    }
  }

  public Map<String, String> toMap() {
    Map<String, String> metadataMap = new TreeMap<>(_simpleFields);
    Map<String, String> customMap = getCustomMap();
    if (customMap != null) {
      try {
        metadataMap.put(Segment.CUSTOM_MAP, JsonUtils.objectToString(customMap));
      } catch (Exception e) {
        LOGGER.error("Caught exception while writing custom map for segment: {}", getSegmentName(), e);
      }
    }
    return metadataMap;
  }

  @Override
  public ZNRecord toZNRecord() {
    // Convert to TreeMap to keep the keys sorted. The de-serialized ZNRecord has simple fields stored as LinkedHashMap.
    if (!(_simpleFields instanceof TreeMap)) {
      _simpleFields = new TreeMap<>(_simpleFields);
      _znRecord.setSimpleFields(_simpleFields);
    }
    return _znRecord;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return toMap().equals(((SegmentZKMetadata) o).toMap());
  }

  @Override
  public int hashCode() {
    return toMap().hashCode();
  }
}
