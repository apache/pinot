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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import org.apache.pinot.client.ConnectionTimeouts;
import org.apache.pinot.client.TlsProtocols;
import org.apache.pinot.spi.utils.CommonConstants;


public class PinotControllerTransportFactory {
  private static final String DEFAULT_CONTROLLER_READ_TIMEOUT = "60000";
  private static final String DEFAULT_CONTROLLER_CONNECT_TIMEOUT = "2000";
  private static final String DEFAULT_CONTROLLER_HANDSHAKE_TIMEOUT = "2000";
  private static final String DEFAULT_CONTROLLER_TLS_V10_ENABLED = "false";

  private Map<String, String> _headers = new HashMap<>();
  private String _scheme = CommonConstants.HTTP_PROTOCOL;
  private SSLContext _sslContext = null;

  private boolean _tlsV10Enabled = false;
  private int _readTimeout = Integer.parseInt(DEFAULT_CONTROLLER_READ_TIMEOUT);
  private int _connectTimeout = Integer.parseInt(DEFAULT_CONTROLLER_CONNECT_TIMEOUT);
  private int _handshakeTimeout = Integer.parseInt(DEFAULT_CONTROLLER_HANDSHAKE_TIMEOUT);

  public PinotControllerTransport buildTransport() {
    ConnectionTimeouts connectionTimeouts = ConnectionTimeouts.create(_readTimeout, _connectTimeout, _handshakeTimeout);
    TlsProtocols tlsProtocols = TlsProtocols.defaultProtocols(_tlsV10Enabled);
    return new PinotControllerTransport(_headers, _scheme, _sslContext, connectionTimeouts, tlsProtocols);
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

  public SSLContext getSslContext() {
    return _sslContext;
  }

  public void setSslContext(SSLContext sslContext) {
    _sslContext = sslContext;
  }

  public PinotControllerTransportFactory withConnectionProperties(Properties properties) {
    _readTimeout = Integer.parseInt(properties.getProperty("controllerReadTimeout",
            DEFAULT_CONTROLLER_READ_TIMEOUT));
    _connectTimeout = Integer.parseInt(properties.getProperty("controllerConnectTimeout",
            DEFAULT_CONTROLLER_CONNECT_TIMEOUT));
    _handshakeTimeout = Integer.parseInt(properties.getProperty("controllerHandshakeTimeout",
            DEFAULT_CONTROLLER_HANDSHAKE_TIMEOUT));
    _tlsV10Enabled = Boolean.parseBoolean(properties.getProperty("controllerTlsV10Enabled",
            DEFAULT_CONTROLLER_TLS_V10_ENABLED))
            || Boolean.parseBoolean(System.getProperties().getProperty("controller.tlsV10Enabled",
            DEFAULT_CONTROLLER_TLS_V10_ENABLED));
    return this;
  }
}
