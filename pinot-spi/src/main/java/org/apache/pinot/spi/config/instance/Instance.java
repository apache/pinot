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
package org.apache.pinot.spi.config.instance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


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
public class Instance extends BaseJsonConfig {
  private final String _host;
  private final int _port;
  private final InstanceType _type;
  private final List<String> _tags;
  private final Map<String, Integer> _pools;

  @JsonCreator
  public Instance(@JsonProperty(value = "host", required = true) String host,
      @JsonProperty(value = "port", required = true) int port,
      @JsonProperty(value = "type", required = true) InstanceType type,
      @JsonProperty("tags") @Nullable List<String> tags, @JsonProperty("pools") @Nullable Map<String, Integer> pools) {
    Preconditions.checkArgument(host != null, "'host' must be configured");
    Preconditions.checkArgument(type != null, "'type' must be configured");
    _host = host;
    _port = port;
    _type = type;
    _tags = tags;
    _pools = pools;
  }

  public String getHost() {
    return _host;
  }

  public int getPort() {
    return _port;
  }

  public InstanceType getType() {
    return _type;
  }

  @Nullable
  public List<String> getTags() {
    return _tags;
  }

  @Nullable
  public Map<String, Integer> getPools() {
    return _pools;
  }
}
