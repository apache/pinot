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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A simplified version of SegmentZKMetadata designed for easy serialization and deserialization.
 * This class maintains only the essential fields from SegmentZKMetadata while providing JSON
 * serialization support via Jackson annotations.
 *
 * Use cases:
 * 1. Serializing segment metadata for API responses
 * 2. Transferring segment metadata between services
 * 3. Storing segment metadata in a format that's easy to deserialize
 *
 * Example usage:
 * SimpleSegmentMetadata metadata = SimpleSegmentMetadata.fromZKMetadata(zkMetadata);
 * String json = JsonUtils.objectToString(metadata);
 * SimpleSegmentMetadata deserialized = objectMapper.readValue(json, SimpleSegmentMetadata.class);
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SimpleSegmentMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleSegmentMetadata.class);
  private final String _segmentName;
  private final Map<String, String> _simpleFields;

  @JsonCreator
  public SimpleSegmentMetadata(
      @JsonProperty("segmentName") String segmentName,
      @JsonProperty("simpleFields") Map<String, String> simpleFields) {
    _segmentName = segmentName;
    _simpleFields = new HashMap<>(simpleFields);
  }

  @JsonGetter
  public String getSegmentName() {
    return _segmentName;
  }

  @JsonGetter
  public Map<String, String> getSimpleFields() {
    return Collections.unmodifiableMap(_simpleFields);
  }

  public long getStartTimeMs() {
    long startTimeMs = -1;
    String startTimeString = _simpleFields.get(CommonConstants.Segment.START_TIME);
    if (startTimeString != null) {
      long startTime = Long.parseLong(startTimeString);
      if (startTime > 0) {
        // NOTE: Need to check whether the start time is positive because some old segment ZK metadata contains negative
        //       start time and null time unit
        startTimeMs = TimeUnit.valueOf(_simpleFields.get(CommonConstants.Segment.TIME_UNIT)).toMillis(startTime);
      }
    }
    return startTimeMs;
  }

  public long getEndTimeMs() {
    long endTimeMs = -1;
    String endTimeString = _simpleFields.get(CommonConstants.Segment.END_TIME);
    if (endTimeString != null) {
      long endTime = Long.parseLong(endTimeString);
      // NOTE: Need to check whether the end time is positive because some old segment ZK metadata contains negative
      //       end time and null time unit
      if (endTime > 0) {
        endTimeMs = TimeUnit.valueOf(_simpleFields.get(CommonConstants.Segment.TIME_UNIT)).toMillis(endTime);
      }
    }
    return endTimeMs;
  }

  public String getIndexVersion() {
    return _simpleFields.get(CommonConstants.Segment.INDEX_VERSION);
  }

  public long getTotalDocs() {
    String value = _simpleFields.get(CommonConstants.Segment.TOTAL_DOCS);
    return value != null ? Long.parseLong(value) : -1;
  }

  public SegmentPartitionMetadata getPartitionMetadata() {
    String partitionMetadataJson = _simpleFields.get(CommonConstants.Segment.PARTITION_METADATA);
    if (partitionMetadataJson != null) {
      try {
        return SegmentPartitionMetadata.fromJsonString(partitionMetadataJson);
      } catch (Exception e) {
        LOGGER.error("Caught exception while reading partition metadata for segment: {}", getSegmentName(), e);
      }
    }
    return null;
  }

  public long getSizeInBytes() {
    String value = _simpleFields.get(CommonConstants.Segment.SIZE_IN_BYTES);
    return value != null ? Long.parseLong(value) : -1;
  }

  public long getCrc() {
    String value = _simpleFields.get(CommonConstants.Segment.CRC);
    return value != null ? Long.parseLong(value) : -1;
  }

  public String getDownloadUrl() {
    String downloadUrl = _simpleFields.get(CommonConstants.Segment.DOWNLOAD_URL);
    // Handle legacy download url keys
    if (downloadUrl == null) {
      downloadUrl = _simpleFields.get(CommonConstants.Segment.Offline.DOWNLOAD_URL);
      if (downloadUrl == null) {
        downloadUrl = _simpleFields.get(CommonConstants.Segment.Realtime.DOWNLOAD_URL);
      }
    }
    return downloadUrl;
  }

  /**
   * Creates a SimpleSegmentMetadata instance from a SegmentZKMetadata object.
   * This method copies all simple fields from the ZK metadata while maintaining
   * the immutability guarantees of this class.
   */
  public static SimpleSegmentMetadata fromZKMetadata(SegmentZKMetadata zkMetadata) {
    return new SimpleSegmentMetadata(
        zkMetadata.getSegmentName(),
        zkMetadata.toMap()
    );
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SimpleSegmentMetadata that = (SimpleSegmentMetadata) o;
    return Objects.equals(_segmentName, that._segmentName)
        && Objects.equals(_simpleFields, that._simpleFields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_segmentName, _simpleFields);
  }
}
