/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.upload.batch;

import com.fasterxml.jackson.annotation.JsonProperty;


public class SegmentEntry {
  @JsonProperty("name")
  private String _segmentName;

  @JsonProperty("size")
  private long _segmentSize;

  public SegmentEntry(@JsonProperty("name") String segmentName, @JsonProperty("size") long segmentSize) {
    _segmentName = segmentName;
    _segmentSize = segmentSize;
  }

  @JsonProperty("name")
  public String getSegmentName() {
    return _segmentName;
  }

  @JsonProperty("size")
  public long getSegmentSize() {
    return _segmentSize;
  }
}
