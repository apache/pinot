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


public class ValidDocIdsBitmapResponse {
  private final String _segmentName;
  private final String _crc;
  private final byte[] _bitmap;

  public ValidDocIdsBitmapResponse(@JsonProperty("segmentName") String segmentName, @JsonProperty("crc") String crc,
      @JsonProperty("bitmap") byte[] bitmap) {
    _segmentName = segmentName;
    _crc = crc;
    _bitmap = bitmap;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public String getCrc() {
    return _crc;
  }

  public byte[] getBitmap() {
    return _bitmap;
  }
}
