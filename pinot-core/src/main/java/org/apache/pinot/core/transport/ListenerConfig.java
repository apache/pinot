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
 * Provides configuration settings expected by an Http Server to 
 * setup listeners for http and https protocols.
 */
public class ListenerConfig {
  private final String _name;
  private final String _host;
  private final int _port;
  private final String _protocol;
  private final TlsConfig _tlsConfig;

  public ListenerConfig(String name, String host, int port, String protocol, TlsConfig tlsConfig) {
    this._name = name;
    this._host = host;
    this._port = port;
    this._protocol = protocol;
    this._tlsConfig = tlsConfig;
  }

  public String getName() {
    return _name;
  }

  public String getHost() {
    return _host;
  }

  public int getPort() {
    return _port;
  }

  public String getProtocol() {
    return _protocol;
  }

  public TlsConfig getTlsConfig() {
    return _tlsConfig;
  }
}
