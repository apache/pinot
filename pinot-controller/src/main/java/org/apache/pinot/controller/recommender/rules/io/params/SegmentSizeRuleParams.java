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

package org.apache.pinot.controller.recommender.rules.io.params;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;


/**
 * Parameters used in SegmentSizeRule
 */
public class SegmentSizeRuleParams {

  // Desired segment size in MB
  private int desiredSegmentSizeMB = RecommenderConstants.SegmentSizeRule.DEFAULT_DESIRED_SEGMENT_SIZE_MB;

  // Number for rows in the generated segment
  private int numRowsInGeneratedSegment = RecommenderConstants.DEFAULT_NUM_ROWS_IN_GENERATED_SEGMENT;

  // Actual segment size in MB
  private int actualSegmentSizeMB = RecommenderConstants.SegmentSizeRule.NOT_PROVIDED;

  // Number of rows in the actual segment
  private int numRowsInActualSegment = RecommenderConstants.SegmentSizeRule.NOT_PROVIDED;

  // setter and getters

  public int getDesiredSegmentSizeMB() {
    return desiredSegmentSizeMB;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setDesiredSegmentSizeMB(int desiredSegmentSizeMB) {
    this.desiredSegmentSizeMB = desiredSegmentSizeMB;
  }

  public int getNumRowsInGeneratedSegment() {
    return numRowsInGeneratedSegment;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNumRowsInGeneratedSegment(int numRowsInGeneratedSegment) {
    this.numRowsInGeneratedSegment = numRowsInGeneratedSegment;
  }

  public int getActualSegmentSizeMB() {
    return actualSegmentSizeMB;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setActualSegmentSizeMB(int actualSegmentSizeMB) {
    this.actualSegmentSizeMB = actualSegmentSizeMB;
  }

  public int getNumRowsInActualSegment() {
    return numRowsInActualSegment;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNumRowsInActualSegment(int numRowsInActualSegment) {
    this.numRowsInActualSegment = numRowsInActualSegment;
  }
}
