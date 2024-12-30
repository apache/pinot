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
package org.apache.pinot.server.api.resources.reingestion;

public class ReIngestionRequest {
  private String _tableNameWithType;
  private String _segmentName;
  private String _uploadURI;
  private boolean _uploadSegment;
  private String _authToken;

  // Getters and setters
  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public void setTableNameWithType(String tableNameWithType) {
    this._tableNameWithType = tableNameWithType;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public void setSegmentName(String segmentName) {
    this._segmentName = segmentName;
  }

  public String getUploadURI() {
    return _uploadURI;
  }

  public void setUploadURI(String uploadURI) {
    this._uploadURI = uploadURI;
  }

  public boolean isUploadSegment() {
    return _uploadSegment;
  }

  public void setUploadSegment(boolean uploadSegment) {
    _uploadSegment = uploadSegment;
  }

  public String getAuthToken() {
    return _authToken;
  }

  public void setAuthToken(String authToken) {
    _authToken = authToken;
  }
}
