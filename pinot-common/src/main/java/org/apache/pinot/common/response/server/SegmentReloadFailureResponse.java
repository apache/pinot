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

import org.apache.pinot.spi.annotations.InterfaceStability;


/**
 * DTO representing a single segment reload failure.
 */
@InterfaceStability.Unstable
public class SegmentReloadFailureResponse {
  private String _segmentName;
  private String _serverName;
  private String _errorMessage;
  private String _stackTrace;
  private long _failedAtMs;

  // Default constructor for JSON deserialization
  public SegmentReloadFailureResponse() {
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public SegmentReloadFailureResponse setSegmentName(String segmentName) {
    _segmentName = segmentName;
    return this;
  }

  public String getServerName() {
    return _serverName;
  }

  public SegmentReloadFailureResponse setServerName(String serverName) {
    _serverName = serverName;
    return this;
  }

  public String getErrorMessage() {
    return _errorMessage;
  }

  public SegmentReloadFailureResponse setErrorMessage(String errorMessage) {
    _errorMessage = errorMessage;
    return this;
  }

  public String getStackTrace() {
    return _stackTrace;
  }

  public SegmentReloadFailureResponse setStackTrace(String stackTrace) {
    _stackTrace = stackTrace;
    return this;
  }

  public long getFailedAtMs() {
    return _failedAtMs;
  }

  public SegmentReloadFailureResponse setFailedAtMs(long failedAtMs) {
    _failedAtMs = failedAtMs;
    return this;
  }

  @Override
  public String toString() {
    return "SegmentReloadFailureDto{"
        + "segmentName='" + _segmentName + '\''
        + ", serverName='" + _serverName + '\''
        + ", errorMessage='" + _errorMessage + '\''
        + ", failedAtMs=" + _failedAtMs
        + '}';
  }
}
