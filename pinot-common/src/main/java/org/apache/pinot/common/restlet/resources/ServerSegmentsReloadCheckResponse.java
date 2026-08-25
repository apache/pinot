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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.annotation.Nullable;


/// Server response for /segments/needReload. `segmentsNeedingReload` is populated only when the
/// endpoint is invoked with `verbose=true`.
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServerSegmentsReloadCheckResponse {
  @JsonProperty("needReload")
  private final boolean _needReload;

  @JsonProperty("instanceId")
  private final String _instanceId;

  @Nullable
  @JsonProperty("segmentsNeedingReload")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final List<String> _segmentsNeedingReload;

  public boolean isNeedReload() {
    return _needReload;
  }

  public String getInstanceId() {
    return _instanceId;
  }

  @Nullable
  public List<String> getSegmentsNeedingReload() {
    return _segmentsNeedingReload;
  }

  @JsonCreator
  public ServerSegmentsReloadCheckResponse(@JsonProperty("needReload") boolean needReload,
      @JsonProperty("instanceId") String instanceId,
      @JsonProperty("segmentsNeedingReload") @Nullable List<String> segmentsNeedingReload) {
    _needReload = needReload;
    _instanceId = instanceId;
    _segmentsNeedingReload = segmentsNeedingReload;
  }

  public ServerSegmentsReloadCheckResponse(boolean needReload, String instanceId) {
    this(needReload, instanceId, null);
  }
}
