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
package org.apache.pinot.server.predownload;

enum PredownloadCompletionReason {
  INSTANCE_NOT_ALIVE(false, false, "%s is not alive in cluster %s"),
  INSTANCE_NON_EXISTENT(false, false, "%s does not exist in cluster %s"),
  CANNOT_CONNECT_TO_DEEPSTORE(false, true, "cannot connect to deepstore for %s in cluster %s"),
  SOME_SEGMENTS_DOWNLOAD_FAILED(false, true, "some segments failed to predownload for %s in cluster %s"),
  NO_SEGMENT_TO_PREDOWNLOAD("no segment to predownload for %s in cluster %s"),
  ALL_SEGMENTS_DOWNLOADED("all segments are predownloaded for %s in cluster %s");

  private static final String FAIL_MESSAGE = "Failed to predownload segments for %s.%s, because ";
  private static final String SUCCESS_MESSAGE = "Successfully predownloaded segments for %s.%s, because ";
  private final boolean _retriable; // Whether the failure is retriable.
  private final boolean _isSucceed; // Whether the predownload is successful.
  private final String _message;
  private final String _messageTemplate;

  PredownloadCompletionReason(boolean isSucceed, boolean retriable, String message) {
    _isSucceed = isSucceed;
    _messageTemplate = isSucceed ? SUCCESS_MESSAGE : FAIL_MESSAGE;
    _retriable = retriable;
    _message = message;
  }

  PredownloadCompletionReason(String message) {
    this(true, false, message);
  }

  public boolean isRetriable() {
    return _retriable;
  }

  public boolean isSucceed() {
    return _isSucceed;
  }

  public String getMessage(String clusterName, String instanceName, String segmentName) {
    return String.format(_messageTemplate + _message, instanceName, segmentName, instanceName, clusterName);
  }
}
