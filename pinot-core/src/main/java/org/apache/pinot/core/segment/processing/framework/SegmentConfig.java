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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;


/**
 * Config for the final segment generation phase of the SegmentProcessorFramework
 */
@JsonDeserialize(builder = SegmentConfig.Builder.class)
public class SegmentConfig {

  private static final int DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT = 1_000_000;
  private final int _maxNumRecordsPerSegment;

  private SegmentConfig(int maxNumRecordsPerSegment) {
    _maxNumRecordsPerSegment = maxNumRecordsPerSegment;
  }

  /**
   * The max number of records allowed per segment
   */
  public int getMaxNumRecordsPerSegment() {
    return _maxNumRecordsPerSegment;
  }

  /**
   * Builder for SegmentConfig
   */
  @JsonPOJOBuilder(withPrefix = "set")
  public static class Builder {
    private int maxNumRecordsPerSegment = DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT;

    public Builder setMaxNumRecordsPerSegment(int maxNumRecordsPerSegment) {
      this.maxNumRecordsPerSegment = maxNumRecordsPerSegment;
      return this;
    }

    public SegmentConfig build() {
      return new SegmentConfig(maxNumRecordsPerSegment);
    }
  }

  @Override
  public String toString() {
    return "SegmentsConfig{" + "_maxNumRecordsPerSegment=" + _maxNumRecordsPerSegment + '}';
  }
}
