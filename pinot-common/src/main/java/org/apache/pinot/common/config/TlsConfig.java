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

import io.netty.handler.ssl.SslProvider;
import java.security.KeyStore;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;


/**
 * Container object for TLS/SSL configuration of pinot clients and servers (netty, grizzly, etc.)
 */
@Getter
@Setter
@EqualsAndHashCode
public class TlsConfig {
  private boolean _clientAuthEnabled;
  private String _keyStoreType = KeyStore.getDefaultType();
  private String _keyStorePath;
  private String _keyStorePassword;
  private String _trustStoreType = KeyStore.getDefaultType();
  private String _trustStorePath;
  private String _trustStorePassword;
  private String _sslProvider = SslProvider.JDK.toString();
  // If true, the client will not verify the server's certificate
  private boolean _insecure = false;

  public TlsConfig() {
    // left blank
  }

  public TlsConfig(TlsConfig tlsConfig) {
    _clientAuthEnabled = tlsConfig._clientAuthEnabled;
    _keyStoreType = tlsConfig._keyStoreType;
    _keyStorePath = tlsConfig._keyStorePath;
    _keyStorePassword = tlsConfig._keyStorePassword;
    _trustStoreType = tlsConfig._trustStoreType;
    _trustStorePath = tlsConfig._trustStorePath;
    _trustStorePassword = tlsConfig._trustStorePassword;
    _sslProvider = tlsConfig._sslProvider;
  }

  public boolean isCustomized() {
    return StringUtils.isNoneBlank(_keyStorePath) || StringUtils.isNoneBlank(_trustStorePath);
  }
}
