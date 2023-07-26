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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Request object for startReplaceSegments API.
 *
 * 1. segmentsFrom : original segments. This field can be empty in case the user tries to upload the original segments
 *    and wants to achieve the atomic update of multiple segments.
 * 2. segmentsTo : merged segments.
 * 3. customMap : custom map.
 */
public class StartReplaceSegmentsRequest {
  private final List<String> _segmentsFrom;
  private final List<String> _segmentsTo;
  private final Map<String, String> _customMap;

  public StartReplaceSegmentsRequest(@JsonProperty("segmentsFrom") @Nullable List<String> segmentsFrom,
      @JsonProperty("segmentsTo") @Nullable List<String> segmentsTo) {
    this(segmentsFrom, segmentsTo, null);
  }

  @JsonCreator
  public StartReplaceSegmentsRequest(@JsonProperty("segmentsFrom") @Nullable List<String> segmentsFrom,
      @JsonProperty("segmentsTo") @Nullable List<String> segmentsTo,
      @JsonProperty("customMap") @Nullable Map<String, String> customMap) {
    _segmentsFrom = (segmentsFrom == null) ? Collections.emptyList() : segmentsFrom;
    _segmentsTo = (segmentsTo == null) ? Collections.emptyList() : segmentsTo;
    Preconditions.checkArgument(!_segmentsFrom.isEmpty() || !_segmentsTo.isEmpty(),
        "'segmentsFrom' and 'segmentsTo' cannot both be empty");
    _customMap = customMap;
  }

  public List<String> getSegmentsFrom() {
    return _segmentsFrom;
  }

  public List<String> getSegmentsTo() {
    return _segmentsTo;
  }

  public Map<String, String> getCustomMap() {
    return _customMap;
  }
}
