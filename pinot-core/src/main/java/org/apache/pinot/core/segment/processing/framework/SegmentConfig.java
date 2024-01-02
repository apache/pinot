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
package org.apache.pinot.core.segment.processing.framework;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;


/**
 * Config for the final segment generation phase of the SegmentProcessorFramework
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentConfig {
  public static final int DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT = 5_000_000;

  private final int _maxNumRecordsPerSegment;
  private final String _segmentNamePrefix;
  private final String _segmentNamePostfix;
  private final String _fixedSegmentName;
  private final String _intermediateFileSizeThresholdInBytes;

  @JsonCreator
  private SegmentConfig(@JsonProperty(value = "maxNumRecordsPerSegment", required = true) int maxNumRecordsPerSegment,
      @JsonProperty("segmentNamePrefix") @Nullable String segmentNamePrefix,
      @JsonProperty("segmentNamePostfix") @Nullable String segmentNamePostfix,
      @JsonProperty("fixedSegmentName") @Nullable String fixedSegmentName,
      @JsonProperty("intermediateFileSizeThreshold") @Nullable String intermediateFileSizeThresholdInBytes) {
    Preconditions.checkState(maxNumRecordsPerSegment > 0, "Max num records per segment must be > 0");
    _maxNumRecordsPerSegment = maxNumRecordsPerSegment;
    _segmentNamePrefix = segmentNamePrefix;
    _segmentNamePostfix = segmentNamePostfix;
    _fixedSegmentName = fixedSegmentName;
    _intermediateFileSizeThresholdInBytes = intermediateFileSizeThresholdInBytes;
  }

  /**
   * The max number of records allowed per segment
   */
  public int getMaxNumRecordsPerSegment() {
    return _maxNumRecordsPerSegment;
  }

  @Nullable
  public String getSegmentNamePrefix() {
    return _segmentNamePrefix;
  }

  @Nullable
  public String getSegmentNamePostfix() {
    return _segmentNamePostfix;
  }

  @Nullable
  public String getFixedSegmentName() {
    return _fixedSegmentName;
  }
  @Nullable
  public String getIntermediateFileSizeThreshold() {
    return _intermediateFileSizeThresholdInBytes;
  }

  /**
   * Builder for SegmentConfig
   */
  public static class Builder {
    private int _maxNumRecordsPerSegment = DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT;
    private String _segmentNamePrefix;
    private String _segmentNamePostfix;
    private String _fixedSegmentName;
    private String _intermediateFileSizeThresholdInBytes = Long.toString(1000000000L);

    public Builder setMaxNumRecordsPerSegment(int maxNumRecordsPerSegment) {
      _maxNumRecordsPerSegment = maxNumRecordsPerSegment;
      return this;
    }

    public Builder setSegmentNamePrefix(String segmentNamePrefix) {
      _segmentNamePrefix = segmentNamePrefix;
      return this;
    }

    public Builder setSegmentNamePostfix(String segmentNamePostfix) {
      _segmentNamePostfix = segmentNamePostfix;
      return this;
    }

    public Builder setFixedSegmentName(String fixedSegmentName) {
      _fixedSegmentName = fixedSegmentName;
      return this;
    }
    public Builder setIntermediateFileSizeThreshold(String intermediateFileSizeThresholdInBytes) {
      _intermediateFileSizeThresholdInBytes = intermediateFileSizeThresholdInBytes;
      return this;
    }

    public SegmentConfig build() {
      Preconditions.checkState(_maxNumRecordsPerSegment > 0, "Max num records per segment must be > 0");
      return new SegmentConfig(_maxNumRecordsPerSegment, _segmentNamePrefix, _segmentNamePostfix, _fixedSegmentName,
          _intermediateFileSizeThresholdInBytes);
    }
  }

  @Override
  public String toString() {
    return "SegmentConfig{" + "_maxNumRecordsPerSegment=" + _maxNumRecordsPerSegment
        + ",_intermediateFileSizeThresholdInBytes=" + _intermediateFileSizeThresholdInBytes + ", _segmentNamePrefix='"
        + _segmentNamePrefix + '\'' + ", _segmentNamePostfix='" + _segmentNamePostfix + '\'' + ", _fixedSegmentName='"
        + _fixedSegmentName + '\'' + '}';
  }
}
