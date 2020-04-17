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
package org.apache.pinot.core.data.manager.realtime;

public class SegmentUploadStatus {
  // True if and only if the upload is successful.
  private boolean uploadSuccessful;
  // Segment location uri string when the upload is successful.
  private String segmentLocation;

  public SegmentUploadStatus(boolean uploadSuccessful, String segmentLocation) {
    this.uploadSuccessful = uploadSuccessful;
    this.segmentLocation = segmentLocation;
  }

  public boolean isUploadSuccessful() {
    return uploadSuccessful;
  }

  public void setUploadSuccessful(boolean uploadSuccessful) {
    this.uploadSuccessful = uploadSuccessful;
  }

  public String getSegmentLocation() {
    return segmentLocation;
  }

  public void setSegmentLocation(String segmentLocation) {
    this.segmentLocation = segmentLocation;
  }
}
