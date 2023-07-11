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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;


@ApiModel
public class InstanceTagUpdateRequest {
  @JsonProperty("instanceName")
  @ApiModelProperty(example = "Server_a.b.com_20000")
  private String _instanceName;
  @JsonProperty("newTags")
  private List<String> _newTags;

  public String getInstanceName() {
    return _instanceName;
  }

  public void setInstanceName(String instanceName) {
    _instanceName = instanceName;
  }

  public List<String> getNewTags() {
    return _newTags;
  }

  public void setNewTags(List<String> newTags) {
    _newTags = newTags;
  }
}
