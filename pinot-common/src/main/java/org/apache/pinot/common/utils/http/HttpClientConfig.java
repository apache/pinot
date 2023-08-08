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

import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.env.PinotConfiguration;


public class HttpClientConfig {
  // Default config uses default values which are same as what Apache Commons Http-Client uses.
  public static final HttpClientConfig DEFAULT_HTTP_CLIENT_CONFIG = HttpClientConfig.newBuilder().build();

  protected static final String MAX_CONNS_CONFIG_NAME = "http.client.maxConnTotal";
  protected static final String MAX_CONNS_PER_ROUTE_CONFIG_NAME = "http.client.maxConnPerRoute";
  protected static final String DISABLE_DEFAULT_USER_AGENT_CONFIG_NAME = "http.client.disableDefaultUserAgent";

  private final int _maxConnTotal;
  private final int _maxConnPerRoute;
  private final boolean _disableDefaultUserAgent;

  private HttpClientConfig(int maxConnTotal, int maxConnPerRoute, boolean disableDefaultUserAgent) {
    _maxConnTotal = maxConnTotal;
    _maxConnPerRoute = maxConnPerRoute;
    _disableDefaultUserAgent = disableDefaultUserAgent;
  }

  public int getMaxConnTotal() {
    return _maxConnTotal;
  }

  public int getMaxConnPerRoute() {
    return _maxConnPerRoute;
  }

  public boolean isDisableDefaultUserAgent() {
    return _disableDefaultUserAgent;
  }

  /**
   * Creates a {@link HttpClientConfig.Builder} and initializes it with relevant configs from the provided
   * configuration. Since http-clients are used in a bunch of places in the code, each use-case can have their own
   * prefix for their config. The caller should call {@link PinotConfiguration#subset(String)} to remove their prefix
   * and this builder will look for exact matches of its relevant configs.
   */
  public static Builder newBuilder(PinotConfiguration pinotConfiguration) {
    Builder builder = new Builder();
    String maxConns = pinotConfiguration.getProperty(MAX_CONNS_CONFIG_NAME);
    if (StringUtils.isNotEmpty(maxConns)) {
      builder.withMaxConns(Integer.parseInt(maxConns));
    }
    String maxConnsPerRoute = pinotConfiguration.getProperty(MAX_CONNS_PER_ROUTE_CONFIG_NAME);
    if (StringUtils.isNotEmpty(maxConnsPerRoute)) {
      builder.withMaxConnsPerRoute(Integer.parseInt(maxConnsPerRoute));
    }
    boolean disableDefaultUserAgent = pinotConfiguration.getProperty(DISABLE_DEFAULT_USER_AGENT_CONFIG_NAME, false);
    builder.withDisableDefaultUserAgent(disableDefaultUserAgent);
    return builder;
  }

  private static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private int _maxConns = -1;
    private int _maxConnsPerRoute = -1;
    private boolean _disableDefaultUserAgent = false;

    private Builder() {
    }

    public Builder withMaxConns(int maxConns) {
      _maxConns = maxConns;
      return this;
    }

    public Builder withMaxConnsPerRoute(int maxConnsPerRoute) {
      _maxConnsPerRoute = maxConnsPerRoute;
      return this;
    }

    public Builder withDisableDefaultUserAgent(boolean disableDefaultUserAgent) {
      _disableDefaultUserAgent = disableDefaultUserAgent;
      return this;
    }

    public HttpClientConfig build() {
      return new HttpClientConfig(_maxConns, _maxConnsPerRoute, _disableDefaultUserAgent);
    }
  }
}
