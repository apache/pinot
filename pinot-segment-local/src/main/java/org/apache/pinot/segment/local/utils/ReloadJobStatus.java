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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.response.server.SegmentReloadFailureResponse;


/// Tracks status of a reload job.
/// Thread-safe for concurrent access.
public class ReloadJobStatus {
  private final String _jobId;
  private final AtomicInteger _failureCount = new AtomicInteger(0);
  // Names of the segments an only-if-needed reload left in place because their loaded copy was already up to date.
  // Kept by name (rather than as a count) so the status check can tell a skipped segment apart from one that a later
  // operation reloaded, and so a skipped segment that was since dropped is not counted.
  private final Set<String> _skippedSegments = ConcurrentHashMap.newKeySet();
  private final long _createdTimeMs;
  private final List<SegmentReloadFailureResponse> _failedSegmentDetails = new ArrayList<>();

  public ReloadJobStatus(String jobId) {
    _jobId = jobId;
    _createdTimeMs = System.currentTimeMillis();
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

  public boolean hasSkippedSegments() {
    return !_skippedSegments.isEmpty();
  }

  public boolean isSkipped(String segmentName) {
    return _skippedSegments.contains(segmentName);
  }

  public void addSkippedSegments(Collection<String> segmentNames) {
    _skippedSegments.addAll(segmentNames);
  }

  public long getCreatedTimeMs() {
    return _createdTimeMs;
  }

  public synchronized List<SegmentReloadFailureResponse> getFailedSegmentDetails() {
    return Collections.unmodifiableList(_failedSegmentDetails);
  }

  synchronized void addFailureDetail(SegmentReloadFailureResponse failureResponse) {
    _failedSegmentDetails.add(failureResponse);
  }

  @Override
  public String toString() {
    return "ReloadJobStatus{jobId='" + _jobId + "', failureCount=" + _failureCount.get()
        + ", skippedCount=" + _skippedSegments.size()
        + ", failedSegmentDetailsCount=" + _failedSegmentDetails.size()
        + ", createdTimeMs=" + _createdTimeMs + '}';
  }
}
