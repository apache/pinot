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
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;


/**
 * This class gives list of the details from each server if there exists any segments that need to be reloaded
 *
 * It has details of reload flag which returns true if reload is needed on table and additional details of the
 * respective servers.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableSegmentsReloadCheckResponse {
  @JsonProperty("needReload")
  boolean _needReload;
  @JsonProperty("serverToSegmentsCheckReloadList")
  Map<String, ServerSegmentsReloadCheckResponse> _serverToSegmentsCheckReloadList;

  public boolean isNeedReload() {
    return _needReload;
  }

  public Map<String, ServerSegmentsReloadCheckResponse> getServerToSegmentsCheckReloadList() {
    return _serverToSegmentsCheckReloadList;
  }

  @JsonCreator
  public TableSegmentsReloadCheckResponse(@JsonProperty("needReload") boolean needReload,
      @JsonProperty("serverToSegmentsCheckReloadList")
      Map<String, ServerSegmentsReloadCheckResponse> serverToSegmentsCheckReloadList) {
    _needReload = needReload;
    _serverToSegmentsCheckReloadList = serverToSegmentsCheckReloadList;
  }
}
