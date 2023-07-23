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
package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.spi.stream.ConsumerPartitionState;


/**
 * Information regarding the consumer of a segment
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentConsumerInfo {
  private final String _segmentName;
  private final String _consumerState;
  private final long _lastConsumedTimestamp;
  private final Map<String, String> _partitionToOffsetMap;
  private final PartitionOffsetInfo _partitionOffsetInfo;

  public SegmentConsumerInfo(@JsonProperty("segmentName") String segmentName,
      @JsonProperty("consumerState") String consumerState,
      @JsonProperty("lastConsumedTimestamp") long lastConsumedTimestamp,
      @JsonProperty("partitionToOffsetMap") Map<String, String> partitionToOffsetMap,
      @JsonProperty("partitionOffsetInfo") PartitionOffsetInfo partitionOffsetInfo) {
    _segmentName = segmentName;
    _consumerState = consumerState;
    _lastConsumedTimestamp = lastConsumedTimestamp;
    _partitionToOffsetMap = partitionToOffsetMap;
    _partitionOffsetInfo = partitionOffsetInfo;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public String getConsumerState() {
    return _consumerState;
  }

  public long getLastConsumedTimestamp() {
    return _lastConsumedTimestamp;
  }

  public Map<String, String> getPartitionToOffsetMap() {
    return _partitionToOffsetMap;
  }

  public PartitionOffsetInfo getPartitionOffsetInfo() {
    return _partitionOffsetInfo;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static public class PartitionOffsetInfo {
    @JsonProperty("currentOffsets")
    public Map<String, String> _currentOffsets;

    @JsonProperty("recordsLag")
    public Map<String, String> _recordsLag;

    @JsonProperty("latestUpstreamOffsets")
    public Map<String, String> _latestUpstreamOffsets;

    @JsonProperty("availabilityLagMs")
    public Map<String, String> _availabilityLagMs;

    public PartitionOffsetInfo(
        @JsonProperty("currentOffsets") Map<String, String> currentOffsets,
        @JsonProperty("latestUpstreamOffsets") Map<String, String> latestUpstreamOffsets,
        @JsonProperty("recordsLag") Map<String, String> recordsLag,
        @JsonProperty("availabilityLagMs") Map<String, String> availabilityLagMs) {
      _currentOffsets = currentOffsets;
      _latestUpstreamOffsets = latestUpstreamOffsets;
      _recordsLag = recordsLag;
      _availabilityLagMs = availabilityLagMs;
    }

    public static PartitionOffsetInfo createFrom(
            Map<String, String> currentOffsets,
            Map<String, ConsumerPartitionState> partitionStateMap,
            Map<String, String> recordsLag,
            Map<String, String> availabilityLagMs
            ) {
      return new PartitionOffsetInfo(
              currentOffsets,
              partitionStateMap.entrySet().stream().collect(
                      Collectors.toMap(Map.Entry::getKey, e -> {
                        ConsumerPartitionState partitionState = e.getValue();
                        return partitionState.getUpstreamLatestOffset() == null
                                ? "UNKNOWN" : partitionState.getUpstreamLatestOffset().toString();
                      })
              ), recordsLag, availabilityLagMs);
    }

    public Map<String, String> getCurrentOffsets() {
      return _currentOffsets;
    }

    public Map<String, String> getRecordsLag() {
      return _recordsLag;
    }

    public Map<String, String> getLatestUpstreamOffsets() {
      return _latestUpstreamOffsets;
    }

    public Map<String, String> getAvailabilityLagMs() {
      return _availabilityLagMs;
    }
  }
}
