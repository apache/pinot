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


/**
 * Config for the final segment generation phase of the SegmentProcessorFramework
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentConfig {

  private static final int DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT = 5_000_000;
  private static final String DEFAULT_SEGMENT_NAME_GENERATOR_TYPE = "simple";

  private final int _maxNumRecordsPerSegment;

  // Currently, 'simple', 'normalizedDate' are supported
  private final String _segmentNameGeneratorType;
  private final String _segmentPrefix;
  private final String _segmentPostfix;
  private final boolean _excludeSequenceId;

  @JsonCreator
  private SegmentConfig(@JsonProperty(value = "maxNumRecordsPerSegment") int maxNumRecordsPerSegment,
      @JsonProperty(value = "segmentNameGeneratorType") String segmentNameGeneratorType,
      @JsonProperty(value = "segmentPrefix") String segmentPrefix,
      @JsonProperty(value = "segmentPostfix") String segmentPostfix,
      @JsonProperty(value = "excludeSequenceId") boolean excludeSequenceId) {
    Preconditions.checkState(maxNumRecordsPerSegment > 0, "Max num records per segment must be > 0");
    _maxNumRecordsPerSegment = maxNumRecordsPerSegment;
    _segmentNameGeneratorType = segmentNameGeneratorType;
    _segmentPrefix = segmentPrefix;
    _segmentPostfix = segmentPostfix;
    _excludeSequenceId = excludeSequenceId;
  }

  /**
   * The max number of records allowed per segment
   */
  @JsonProperty
  public int getMaxNumRecordsPerSegment() {
    return _maxNumRecordsPerSegment;
  }

  @JsonProperty
  public String getSegmentNameGeneratorType() {
    return _segmentNameGeneratorType;
  }

  @JsonProperty
  public String getSegmentPrefix() {
    return _segmentPrefix;
  }

  @JsonProperty
  public String getSegmentPostfix() {
    return _segmentPostfix;
  }

  @JsonProperty
  public boolean getExcludeSequenceId() {
    return _excludeSequenceId;
  }

  /**
   * Builder for SegmentConfig
   */
  public static class Builder {
    private int _maxNumRecordsPerSegment = DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT;
    private String _segmentNameGeneratorType = DEFAULT_SEGMENT_NAME_GENERATOR_TYPE;
    private String _segmentPrefix;
    private String _segmentPostfix;
    private boolean _excludeSequenceId;

    public Builder setMaxNumRecordsPerSegment(int maxNumRecordsPerSegment) {
      _maxNumRecordsPerSegment = maxNumRecordsPerSegment;
      return this;
    }

    public Builder setSegmentNameGeneratorType(String segmentNameGeneratorType) {
      _segmentNameGeneratorType = segmentNameGeneratorType;
      return this;
    }

    public Builder setSegmentPrefix(String segmentPrefix) {
      _segmentPrefix = segmentPrefix;
      return this;
    }

    public Builder setSegmentPostfix(String segmentPostfix) {
      _segmentPostfix = segmentPostfix;
      return this;
    }

    public Builder setExcludeSequenceId(boolean excludeSequenceId) {
      _excludeSequenceId = excludeSequenceId;
      return this;
    }

    public SegmentConfig build() {
      Preconditions.checkState(_maxNumRecordsPerSegment > 0, "Max num records per segment must be > 0");
      return new SegmentConfig(_maxNumRecordsPerSegment, _segmentNameGeneratorType, _segmentPrefix, _segmentPostfix,
          _excludeSequenceId);
    }
  }

  @Override
  public String toString() {
    return "SegmentConfig{" + "_maxNumRecordsPerSegment=" + _maxNumRecordsPerSegment + ", _segmentNameGeneratorType='"
        + _segmentNameGeneratorType + '\'' + ", _segmentPrefix='" + _segmentPrefix + '\'' + ", _segmentPostfix='"
        + _segmentPostfix + '\'' + ", _excludeSequenceId=" + _excludeSequenceId + '}';
  }
}
