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
package org.apache.pinot.common.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pinot.common.utils.JsonUtils;


/**
 * Base implementation for the JSON based configurations.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class BaseJsonConfig {

  public JsonNode toJsonNode() {
    return JsonUtils.objectToJsonNode(this);
  }

  public String toJsonString() {
    return toJsonNode().toString();
  }

  @Override
  public int hashCode() {
    return toJsonNode().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof BaseJsonConfig) {
      return toJsonNode().equals(((BaseJsonConfig) obj).toJsonNode());
    }
    return false;
  }

  @Override
  public String toString() {
    return toJsonString();
  }
}
