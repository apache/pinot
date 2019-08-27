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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.common.utils.CommonConstants.Helix.InstanceType;
import org.apache.pinot.common.utils.JsonUtils;


/**
 * Instance configuration.
 * <pre>
 * Example:
 * {
 *   "host": "hostname.example.com",
 *   "port": 1234,
 *   "type": "SERVER",
 *   "tags": ["example_OFFLINE"],
 *   "pools": {
 *     "example_OFFLINE": 0
 *   }
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Instance {
  public static final String POOL_KEY = "pool";

  private final String _host;
  private final int _port;
  private final InstanceType _type;
  private final List<String> _tags;
  private final Map<String, Integer> _pools;

  @JsonCreator
  public Instance(@JsonProperty(value = "host", required = true) String host,
      @JsonProperty(value = "port", required = true) int port,
      @JsonProperty(value = "type", required = true) InstanceType type,
      @JsonProperty(value = "tags") @Nullable List<String> tags,
      @JsonProperty(value = "pools") @Nullable Map<String, Integer> pools) {
    _host = host;
    _port = port;
    _type = type;
    _tags = tags;
    _pools = pools;
  }

  @JsonProperty
  public String getHost() {
    return _host;
  }

  @JsonProperty
  public int getPort() {
    return _port;
  }

  @JsonProperty
  public InstanceType getType() {
    return _type;
  }

  @JsonProperty
  public List<String> getTags() {
    return _tags;
  }

  @JsonProperty
  public Map<String, Integer> getPools() {
    return _pools;
  }

  @JsonIgnore
  public String getInstanceId() {
    String prefix;
    switch (_type) {
      case CONTROLLER:
        prefix = Helix.PREFIX_OF_CONTROLLER_INSTANCE;
        break;
      case BROKER:
        prefix = Helix.PREFIX_OF_BROKER_INSTANCE;
        break;
      case SERVER:
        prefix = Helix.PREFIX_OF_SERVER_INSTANCE;
        break;
      case MINION:
        prefix = Helix.PREFIX_OF_MINION_INSTANCE;
        break;
      default:
        throw new IllegalStateException();
    }
    return prefix + _host + "_" + _port;
  }

  public InstanceConfig toInstanceConfig() {
    InstanceConfig instanceConfig = InstanceConfig.toInstanceConfig(getInstanceId());
    if (_tags != null) {
      for (String tag : _tags) {
        instanceConfig.addTag(tag);
      }
    }
    if (_pools != null && !_pools.isEmpty()) {
      Map<String, String> mapValue = new TreeMap<>();
      for (Map.Entry<String, Integer> entry : _pools.entrySet()) {
        mapValue.put(entry.getKey(), entry.getValue().toString());
      }
      instanceConfig.getRecord().setMapField(POOL_KEY, mapValue);
    }
    return instanceConfig;
  }

  public String toJsonString() {
    try {
      return JsonUtils.objectToString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
