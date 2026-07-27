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

package org.apache.pinot.controller.helix.core.replication;

import com.fasterxml.jackson.annotation.JsonGetter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Tracks the progress of table replication.
 */
public class TableReplicationProgressStats {

  public enum SegmentStatus {
    COMPLETED,
    ERROR,
  }

  private final AtomicInteger _remainingSegments;
  private final BlockingQueue<String> _segmentsFailToCopy = new LinkedBlockingQueue<>();

  public TableReplicationProgressStats(int segmentSize) {
    _remainingSegments = new AtomicInteger(segmentSize);
  }

  /**
   * Updates the status of a segment and returns the number of remaining segments.
   * @param segment The segment name.
   * @param status The status of the segment replication.
   * @return The number of remaining segments to be replicated.
   */
  public int updateSegmentStatus(String segment, SegmentStatus status) {
    if (status == SegmentStatus.ERROR) {
      _segmentsFailToCopy.add(segment);
    }
    return _remainingSegments.addAndGet(-1);
  }

  @JsonGetter("remainingSegments")
  public int getRemainingSegments() {
    return _remainingSegments.get();
  }

  @JsonGetter("segmentsFailToCopy")
  public List<String> getSegmentsFailToCopy() {
    return new ArrayList<>(_segmentsFailToCopy);
  }
}
