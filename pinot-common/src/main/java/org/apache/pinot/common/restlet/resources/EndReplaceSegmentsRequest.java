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
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * segmentsTo: The new segments that actually get created. Sometimes not all segments that are passed into
 * startReplaceSegments can get created. If only a subset of the original list eventually gets created,
 * we need to be able to supply that list to the replacement protocol, so that the remaining
 * segments that did not get created can be ignored.
 */
public class EndReplaceSegmentsRequest {
  private final List<String> _segmentsTo;

  public EndReplaceSegmentsRequest(@JsonProperty("segmentsTo") @Nullable List<String> segmentsTo) {
    _segmentsTo = (segmentsTo == null) ? Collections.emptyList() : segmentsTo;
  }
  public List<String> getSegmentsTo() {
    return _segmentsTo;
  }
}
