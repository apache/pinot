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
package org.apache.pinot.controller.api.resources;

import java.util.List;
import java.util.Map;

public class ServerReloadControllerJobStatusResponse {
  private double _timeElapsedInMinutes;
  private double _estimatedTimeRemainingInMinutes;
  private int _totalSegmentCount;
  private int _successCount;
  private List<String> _pendingSegments;
  private int _totalServersQueried;
  private int _totalServerCallsFailed;
  private Map<String, String> _metadata;

  public int getTotalSegmentCount() {
    return _totalSegmentCount;
  }

  public void setTotalSegmentCount(int totalSegmentCount) {
    _totalSegmentCount = totalSegmentCount;
  }

  public int getSuccessCount() {
    return _successCount;
  }

  public void setSuccessCount(int successCount) {
    _successCount = successCount;
  }

  public List<String> getPendingSegments() {
    return _pendingSegments;
  }

  public void setPendingSegments(List<String> pendingSegments) {
    _pendingSegments = pendingSegments;
  }

  public double getEstimatedTimeRemainingInMinutes() {
    return _estimatedTimeRemainingInMinutes;
  }

  public void setEstimatedTimeRemainingInMinutes(double estimatedTimeRemainingInMillis) {
    _estimatedTimeRemainingInMinutes = estimatedTimeRemainingInMillis;
  }

  public double getTimeElapsedInMinutes() {
    return _timeElapsedInMinutes;
  }

  public void setTimeElapsedInMinutes(double timeElapsedInMinutes) {
    _timeElapsedInMinutes = timeElapsedInMinutes;
  }

  public int getTotalServersQueried() {
    return _totalServersQueried;
  }

  public void setTotalServersQueried(int totalServersQueried) {
    _totalServersQueried = totalServersQueried;
  }

  public int getTotalServerCallsFailed() {
    return _totalServerCallsFailed;
  }

  public void setTotalServerCallsFailed(int totalServerCallsFailed) {
    _totalServerCallsFailed = totalServerCallsFailed;
  }

  public Map<String, String> getMetadata() {
    return _metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    _metadata = metadata;
  }
}
