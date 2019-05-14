/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 *
 */

package org.apache.pinot.thirdeye.detection.spi.model;

import org.apache.pinot.thirdeye.datalayer.dto.EvaluationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;


public class EvaluationSlice {
  private final long start;
  private final long end;
  private final long detectionConfigId;

  public EvaluationSlice(long start, long end, long detectionConfigId) {
    this.start = start;
    this.end = end;
    this.detectionConfigId = detectionConfigId;
  }

  public EvaluationSlice() {
    // -1 means match any
    this(-1, -1, -1);
  }

  public EvaluationSlice withStartTime(long startTime) {
    return new EvaluationSlice(startTime, this.end, this.detectionConfigId);
  }

  public EvaluationSlice withEndTime(long endTime) {
    return new EvaluationSlice(this.start, endTime, this.detectionConfigId);
  }

  public EvaluationSlice withDetectionConfigId(long detectionConfigId) {
    return new EvaluationSlice(this.start, this.end, detectionConfigId);
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public long getDetectionConfigId() {
    return detectionConfigId;
  }

  public boolean match(EvaluationDTO evaluationDTO) {
    if (this.start >= 0 && evaluationDTO.getEndTime() <= this.start)
      return false;
    if (this.end >= 0 && evaluationDTO.getStartTime() >= this.end)
      return false;
    if (this.detectionConfigId >= 0 && evaluationDTO.getDetectionConfigId() != this.detectionConfigId) {
      return false;
    }
    return true;
  }
}
