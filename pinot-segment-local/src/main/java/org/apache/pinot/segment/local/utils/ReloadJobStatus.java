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
package org.apache.pinot.segment.local.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.response.server.SegmentReloadFailure;


/**
 * Tracks status of a reload job.
 */
public class ReloadJobStatus {
  private final String _jobId;
  private final AtomicInteger _failureCount;
  private final long _createdTimeMs;
  private final List<SegmentReloadFailure> _failedSegmentDetails;

  public ReloadJobStatus(String jobId) {
    _jobId = jobId;
    _failureCount = new AtomicInteger(0);
    _createdTimeMs = System.currentTimeMillis();
    _failedSegmentDetails = new ArrayList<>();
  }

  public String getJobId() {
    return _jobId;
  }

  public int getFailureCount() {
    return _failureCount.get();
  }

  public int incrementAndGetFailureCount() {
    return _failureCount.incrementAndGet();
  }

  public long getCreatedTimeMs() {
    return _createdTimeMs;
  }

  public List<SegmentReloadFailure> getFailedSegmentDetails() {
    return _failedSegmentDetails;
  }

  @Override
  public String toString() {
    return "ReloadJobStatus{jobId='" + _jobId + "', failureCount=" + _failureCount.get()
        + ", failedSegmentDetailsCount=" + _failedSegmentDetails.size()
        + ", createdTimeMs=" + _createdTimeMs + '}';
  }
}
