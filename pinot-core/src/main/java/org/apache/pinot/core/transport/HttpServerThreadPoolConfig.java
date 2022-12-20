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
package org.apache.pinot.core.transport;

/**
 * This configures the thread pool configs for the Http servers in Pinot server, controller, broker and minion.
 */
public class HttpServerThreadPoolConfig {
  private static final HttpServerThreadPoolConfig DEFAULT =
      new HttpServerThreadPoolConfig(Runtime.getRuntime().availableProcessors() * 2,
          Runtime.getRuntime().availableProcessors() * 2);
  private int _maxPoolSize;
  private int _corePoolSize;

  public HttpServerThreadPoolConfig(int corePoolSize, int maxPoolSize) {
    _maxPoolSize = maxPoolSize;
    _corePoolSize = corePoolSize;
  }

  public static HttpServerThreadPoolConfig defaultInstance() {
    return DEFAULT.copy();
  }

  public int getMaxPoolSize() {
    return _maxPoolSize;
  }

  public void setMaxPoolSize(int maxPoolSize) {
    _maxPoolSize = maxPoolSize;
  }

  public int getCorePoolSize() {
    return _corePoolSize;
  }

  public void setCorePoolSize(int corePoolSize) {
    _corePoolSize = corePoolSize;
  }

  public HttpServerThreadPoolConfig copy() {
    return new HttpServerThreadPoolConfig(_corePoolSize, _maxPoolSize);
  }
}
