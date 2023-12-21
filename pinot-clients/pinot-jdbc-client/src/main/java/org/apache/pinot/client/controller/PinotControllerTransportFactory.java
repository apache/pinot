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
package org.apache.pinot.client.controller;

import io.netty.handler.ssl.SslContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.pinot.client.ConnectionTimeouts;
import org.apache.pinot.client.TlsProtocols;
import org.apache.pinot.spi.utils.CommonConstants;


public class PinotControllerTransportFactory {
  private static final String DEFAULT_CONTROLLER_READ_TIMEOUT_MS = "60000";
  private static final String DEFAULT_CONTROLLER_CONNECT_TIMEOUT_MS = "2000";
  private static final String DEFAULT_CONTROLLER_HANDSHAKE_TIMEOUT_MS = "2000";
  private static final String DEFAULT_CONTROLLER_TLS_V10_ENABLED = "false";

  private Map<String, String> _headers = new HashMap<>();
  private String _scheme = CommonConstants.HTTP_PROTOCOL;
  private SslContext _sslContext = null;

  private boolean _tlsV10Enabled = false;
  private int _readTimeoutMs = Integer.parseInt(DEFAULT_CONTROLLER_READ_TIMEOUT_MS);
  private int _connectTimeoutMs = Integer.parseInt(DEFAULT_CONTROLLER_CONNECT_TIMEOUT_MS);
  private int _handshakeTimeoutMs = Integer.parseInt(DEFAULT_CONTROLLER_HANDSHAKE_TIMEOUT_MS);
  private String _appId = null;

  public PinotControllerTransport buildTransport() {
    ConnectionTimeouts connectionTimeouts =
        ConnectionTimeouts.create(_readTimeoutMs, _connectTimeoutMs, _handshakeTimeoutMs);
    TlsProtocols tlsProtocols = TlsProtocols.defaultProtocols(_tlsV10Enabled);
    return new PinotControllerTransport(_headers, _scheme, _sslContext, connectionTimeouts, tlsProtocols, _appId);
  }

  public Map<String, String> getHeaders() {
    return _headers;
  }

  public void setHeaders(Map<String, String> headers) {
    _headers = headers;
  }

  public String getScheme() {
    return _scheme;
  }

  public void setScheme(String scheme) {
    _scheme = scheme;
  }

  public SslContext getSslContext() {
    return _sslContext;
  }

  public void setSslContext(SslContext sslContext) {
    _sslContext = sslContext;
  }

  public PinotControllerTransportFactory withConnectionProperties(Properties properties) {
    _readTimeoutMs =
        Integer.parseInt(properties.getProperty("controllerReadTimeoutMs", DEFAULT_CONTROLLER_READ_TIMEOUT_MS));
    _connectTimeoutMs =
        Integer.parseInt(properties.getProperty("controllerConnectTimeoutMs", DEFAULT_CONTROLLER_CONNECT_TIMEOUT_MS));
    _handshakeTimeoutMs = Integer.parseInt(
        properties.getProperty("controllerHandshakeTimeoutMs", DEFAULT_CONTROLLER_HANDSHAKE_TIMEOUT_MS));
    _appId = properties.getProperty("appId");
    _tlsV10Enabled =
        Boolean.parseBoolean(properties.getProperty("controllerTlsV10Enabled", DEFAULT_CONTROLLER_TLS_V10_ENABLED))
            || Boolean.parseBoolean(
            System.getProperties().getProperty("controller.tlsV10Enabled", DEFAULT_CONTROLLER_TLS_V10_ENABLED));
    return this;
  }
}
