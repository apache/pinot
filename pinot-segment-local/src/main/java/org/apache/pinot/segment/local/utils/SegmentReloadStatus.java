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

import org.apache.commons.lang3.exception.ExceptionUtils;

import static java.util.Objects.requireNonNull;


/**
 * Used by ServerReloadJobStatusCache to track failed segments with error details.
 */
public class SegmentReloadStatus {
  private final String _segmentName;
  private final String _errorMessage;
  private final String _stackTrace;
  private final long _failedAtMs;

  /**
   * Creates a new segment reload status capturing failure details.
   *
   * @param segmentName name of the failed segment
   * @param exception the exception that caused the failure
   */
  public SegmentReloadStatus(String segmentName, Throwable exception) {
    _segmentName = requireNonNull(segmentName, "segmentName cannot be null");
    requireNonNull(exception, "exception cannot be null");
    _errorMessage = exception.getMessage();
    _stackTrace = ExceptionUtils.getStackTrace(exception);
    _failedAtMs = System.currentTimeMillis();
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public String getErrorMessage() {
    return _errorMessage;
  }

  public String getStackTrace() {
    return _stackTrace;
  }

  public long getFailedAtMs() {
    return _failedAtMs;
  }

  @Override
  public String toString() {
    return "SegmentReloadStatus{"
        + "segmentName='" + _segmentName + '\''
        + ", errorMessage='" + _errorMessage + '\''
        + ", failedAtMs=" + _failedAtMs
        + '}';
  }
}
