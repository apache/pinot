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

import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Container object for netty configuration of pinot clients and servers (netty, grizzly, etc.)
 */
public class NettyConfig {
  private static final String NATIVE_TRANSPORTS_ENABLED = "native.transports.enabled";
  private boolean _nativeTransportsEnabled = false;

  private static String key(String namespace, String suffix) {
    return namespace + "." + suffix;
  }

  public static NettyConfig extractNettyConfig(PinotConfiguration pinotConfig, String namespace) {
    return NettyConfig.extractNettyConfig(pinotConfig, namespace, new NettyConfig());
  }

  public static NettyConfig extractNettyConfig(PinotConfiguration pinotConfig, String namespace,
      NettyConfig defaultConfig) {

    NettyConfig nettyConfig = new NettyConfig();
    nettyConfig.setNativeTransportsEnabled(pinotConfig.getProperty(key(namespace, NATIVE_TRANSPORTS_ENABLED),
        defaultConfig.isNativeTransportsEnabled()));

    return nettyConfig;
  }

  public boolean isNativeTransportsEnabled() {
    return _nativeTransportsEnabled;
  }

  public void setNativeTransportsEnabled(boolean nativeTransportsEnabled) {
    _nativeTransportsEnabled = nativeTransportsEnabled;
  }
}
