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

package org.apache.pinot.common.response.server;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.InterfaceStability;


@InterfaceStability.Unstable
public class ServerReloadStatusResponse {
  private long _totalSegmentCount;
  private int _successCount;
  private Long _failureCount;
  private List<SegmentReloadFailureResponse> _segmentReloadFailures;

  public long getTotalSegmentCount() {
    return _totalSegmentCount;
  }

  public ServerReloadStatusResponse setTotalSegmentCount(long totalSegmentCount) {
    _totalSegmentCount = totalSegmentCount;
    return this;
  }

  public int getSuccessCount() {
    return _successCount;
  }

  public ServerReloadStatusResponse setSuccessCount(int successCount) {
    _successCount = successCount;
    return this;
  }

  @Nullable
  public Long getFailureCount() {
    return _failureCount;
  }

  public ServerReloadStatusResponse setFailureCount(Long failureCount) {
    _failureCount = failureCount;
    return this;
  }

  @Nullable
  public List<SegmentReloadFailureResponse> getSampleSegmentReloadFailures() {
    return _segmentReloadFailures;
  }

  public ServerReloadStatusResponse setSampleSegmentReloadFailures(
      List<SegmentReloadFailureResponse> sampleSegmentReloadFailureResponses) {
    _segmentReloadFailures = sampleSegmentReloadFailureResponses;
    return this;
  }
}
