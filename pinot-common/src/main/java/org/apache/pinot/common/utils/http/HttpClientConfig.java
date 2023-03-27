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
package org.apache.pinot.common.utils.http;

public class HttpClientConfig {
  // Http-client is used in many places in the code. A caller may choose to add its own prefix to these configs.
  public static final String MAX_CONNS_CONFIG_NAME = "http.client.max_conns";
  public static final String MAX_CONNS_PER_ROUTE_CONFIG_NAME = "http.client.max_conns_per_route";
  // Default config uses default values which are same as what Apache Commons Http-Client uses.
  public static final HttpClientConfig DEFAULT_HTTP_CLIENT_CONFIG = HttpClientConfig.newBuilder().build();
  private final int _maxConns;
  private final int _maxConnsPerRoute;

  HttpClientConfig(int maxConns, int maxConnsPerRoute) {
    _maxConns = maxConns;
    _maxConnsPerRoute = maxConnsPerRoute;
  }

  public int getMaxConns() {
    return _maxConns;
  }

  public int getMaxConnsPerRoute() {
    return _maxConnsPerRoute;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private int _maxConns = 20;
    private int _maxConnsPerRoute = 2;

    Builder() {
    }

    public Builder withMaxConns(int maxConns) {
      _maxConns = maxConns;
      return this;
    }

    public Builder withMaxConnsPerRoute(int maxConnsPerRoute) {
      _maxConnsPerRoute = maxConnsPerRoute;
      return this;
    }

    public HttpClientConfig build() {
      return new HttpClientConfig(_maxConns, _maxConnsPerRoute);
    }
  }
}
