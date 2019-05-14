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


/**
 * Selector for evaluations based on (optionally) start time and end time.
 */
public class EvaluationSlice {
  private final long start;
  private final long end;

  private EvaluationSlice(long start, long end) {
    this.start = start;
    this.end = end;
  }

  public EvaluationSlice() {
    // -1 means match any
    this(-1, -1);
  }

  public EvaluationSlice withStartTime(long startTime) {
    return new EvaluationSlice(startTime, this.end);
  }

  public EvaluationSlice withEndTime(long endTime) {
    return new EvaluationSlice(this.start, endTime);
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public boolean match(EvaluationDTO evaluationDTO) {
    if (this.start >= 0 && evaluationDTO.getEndTime() <= this.start)
      return false;
    if (this.end >= 0 && evaluationDTO.getStartTime() >= this.end)
      return false;
    return true;
  }
}
