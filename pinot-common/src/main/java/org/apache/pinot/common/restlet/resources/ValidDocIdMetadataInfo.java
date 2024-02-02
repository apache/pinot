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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


@JsonIgnoreProperties(ignoreUnknown = true)
public class ValidDocIdMetadataInfo {
  private final String _segmentName;
  private final long _totalValidDocs;
  private final long _totalInvalidDocs;
  private final long _totalDocs;
  private final String _segmentCrc;
  private final String _validDocIdsType;

  public ValidDocIdMetadataInfo(@JsonProperty("segmentName") String segmentName,
      @JsonProperty("totalValidDocs") long totalValidDocs, @JsonProperty("totalInvalidDocs") long totalInvalidDocs,
      @JsonProperty("totalDocs") long totalDocs, @JsonProperty("segmentCrc") String segmentCrc,
      @JsonProperty("validDocIdsType") String validDocIdsType) {
    _segmentName = segmentName;
    _totalValidDocs = totalValidDocs;
    _totalInvalidDocs = totalInvalidDocs;
    _totalDocs = totalDocs;
    _segmentCrc = segmentCrc;
    _validDocIdsType = validDocIdsType;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public long getTotalValidDocs() {
    return _totalValidDocs;
  }

  public long getTotalInvalidDocs() {
    return _totalInvalidDocs;
  }

  public long getTotalDocs() {
    return _totalDocs;
  }

  public String getSegmentCrc() {
    return _segmentCrc;
  }

  public String getValidDocIdsType() {
    return _validDocIdsType;
  }
}
