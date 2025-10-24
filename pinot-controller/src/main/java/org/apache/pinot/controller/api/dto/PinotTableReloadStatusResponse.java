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
package org.apache.pinot.controller.api.dto;

public class PinotTableReloadStatusResponse {
  private double _timeElapsedInMinutes;
  private double _estimatedTimeRemainingInMinutes;
  private int _totalSegmentCount;
  private int _successCount;
  private int _totalServersQueried;
  private int _totalServerCallsFailed;
  private PinotControllerJobMetadataDto _metadata;

  public int getTotalSegmentCount() {
    return _totalSegmentCount;
  }

  public PinotTableReloadStatusResponse setTotalSegmentCount(int totalSegmentCount) {
    _totalSegmentCount = totalSegmentCount;
    return this;
  }

  public int getSuccessCount() {
    return _successCount;
  }

  public PinotTableReloadStatusResponse setSuccessCount(int successCount) {
    _successCount = successCount;
    return this;
  }

  public double getEstimatedTimeRemainingInMinutes() {
    return _estimatedTimeRemainingInMinutes;
  }

  public PinotTableReloadStatusResponse setEstimatedTimeRemainingInMinutes(
      double estimatedTimeRemainingInMinutes) {
    _estimatedTimeRemainingInMinutes = estimatedTimeRemainingInMinutes;
    return this;
  }

  public double getTimeElapsedInMinutes() {
    return _timeElapsedInMinutes;
  }

  public PinotTableReloadStatusResponse setTimeElapsedInMinutes(double timeElapsedInMinutes) {
    _timeElapsedInMinutes = timeElapsedInMinutes;
    return this;
  }


  public int getTotalServersQueried() {
    return _totalServersQueried;
  }

  public PinotTableReloadStatusResponse setTotalServersQueried(int totalServersQueried) {
    _totalServersQueried = totalServersQueried;
    return this;
  }

  public int getTotalServerCallsFailed() {
    return _totalServerCallsFailed;
  }

  public PinotTableReloadStatusResponse setTotalServerCallsFailed(int totalServerCallsFailed) {
    _totalServerCallsFailed = totalServerCallsFailed;
    return this;
  }

  public PinotControllerJobMetadataDto getMetadata() {
    return _metadata;
  }

  public PinotTableReloadStatusResponse setMetadata(PinotControllerJobMetadataDto metadata) {
    _metadata = metadata;
    return this;
  }
}
