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

package org.apache.pinot.controller.recommender.rules.io.configs;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;


/**
 * The recommendations proposed by SegmentSizeRule
 */
public class SegmentSizeRecommendations {

  private long numRowsPerSegment;
  private long numSegments;
  private long segmentSize;
  private String message;

  public SegmentSizeRecommendations(long numRowsPerSegment, long numSegments, long segmentSize) {
    this.numRowsPerSegment = numRowsPerSegment;
    this.numSegments = numSegments;
    this.segmentSize = segmentSize;
  }

  public SegmentSizeRecommendations(String message) {
    this.message = message;
  }

  public SegmentSizeRecommendations() {
  }

  public long getNumRowsPerSegment() {
    return numRowsPerSegment;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNumRowsPerSegment(long numRowsPerSegment) {
    this.numRowsPerSegment = numRowsPerSegment;
  }

  public long getNumSegments() {
    return numSegments;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNumSegments(long numSegments) {
    this.numSegments = numSegments;
  }

  public long getSegmentSize() {
    return segmentSize;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setSegmentSize(long segmentSize) {
    this.segmentSize = segmentSize;
  }

  public String getMessage() {
    return message;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setMessage(String message) {
    this.message = message;
  }
}
