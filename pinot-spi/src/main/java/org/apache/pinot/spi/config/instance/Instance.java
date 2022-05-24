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
 *   },
 *   "grpcPort": 8090
 * }
 * </pre>
 */
public class Instance extends BaseJsonConfig {
  public static final int NOT_SET_GRPC_PORT_VALUE = -1;
  public static final int NOT_SET_ADMIN_PORT_VALUE = -1;
  public static final int NOT_SET_QUERY_SERVER_PORT_VALUE = -1;
  public static final int NOT_SET_QUERY_MAILBOX_PORT_VALUE = -1;

  private final String _host;
  private final int _port;
  private final InstanceType _type;
  private final List<String> _tags;
  private final Map<String, Integer> _pools;
  private final int _grpcPort;
  private final int _adminPort;
  private final int _queryServicePort;
  private final int _queryMailboxPort;
  private final boolean _queriesDisabled;

  @JsonCreator
  public Instance(@JsonProperty(value = "host", required = true) String host,
      @JsonProperty(value = "port", required = true) int port,
      @JsonProperty(value = "type", required = true) InstanceType type,
      @JsonProperty("tags") @Nullable List<String> tags, @JsonProperty("pools") @Nullable Map<String, Integer> pools,
      @JsonProperty("grpcPort") int grpcPort, @JsonProperty("adminPort") int adminPort,
      @JsonProperty("queryServicePort") int queryServicePort, @JsonProperty("queryMailboxPort") int queryMailboxPort,
      @JsonProperty("queriesDisabled") boolean queriesDisabled) {
    Preconditions.checkArgument(host != null, "'host' must be configured");
    Preconditions.checkArgument(type != null, "'type' must be configured");
    _host = host;
    _port = port;
    _type = type;
    _tags = tags;
    _pools = pools;
    if (grpcPort == 0) {
      _grpcPort = NOT_SET_GRPC_PORT_VALUE;
    } else {
      _grpcPort = grpcPort;
    }
    if (adminPort == 0) {
      _adminPort = NOT_SET_ADMIN_PORT_VALUE;
    } else {
      _adminPort = adminPort;
    }
    if (queryServicePort == 0) {
      _queryServicePort = NOT_SET_QUERY_SERVER_PORT_VALUE;
    } else {
      _queryServicePort = queryServicePort;
    }
    if (queryMailboxPort == 0) {
      _queryMailboxPort = NOT_SET_QUERY_MAILBOX_PORT_VALUE;
    } else {
      _queryMailboxPort = queryMailboxPort;
    }
    _queriesDisabled = queriesDisabled;
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

  public int getGrpcPort() {
    return _grpcPort;
  }

  public int getAdminPort() {
    return _adminPort;
  }

  public int getQueryServicePort() {
    return _queryServicePort;
  }

  public int getQueryMailboxPort() {
    return _queryMailboxPort;
  }

  public boolean isQueriesDisabled() {
    return _queriesDisabled;
  }
}
