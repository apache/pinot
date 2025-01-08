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
package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonProperty;


public class TableLLCSegmentUploadResponse {
  private final String _segmentName;
  private final long _crc;
  private final String _downloadUrl;

  public TableLLCSegmentUploadResponse(@JsonProperty("segmentName") String segmentName,
      @JsonProperty("crc") Long crc, @JsonProperty("downloadUrl") String downloadUrl) {
    _segmentName = segmentName;
    _crc = crc;
    _downloadUrl = downloadUrl;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public Long getCrc() {
    return _crc;
  }

  public String getDownloadUrl() {
    return _downloadUrl;
  }
}
