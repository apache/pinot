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
import org.apache.commons.lang3.StringUtils;


/**
 * Container object for TLS/SSL configuration of pinot clients and servers (netty, grizzly, etc.)
 */
public class TlsConfig {
  private boolean _clientAuthEnabled;
  private String _keyStoreType = KeyStore.getDefaultType();
  private String _keyStorePath;
  private String _keyStorePassword;
  private String _trustStoreType = KeyStore.getDefaultType();
  private String _trustStorePath;
  private String _trustStorePassword;
  private String _sslProvider = SslProvider.JDK.toString();
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

  public boolean isClientAuthEnabled() {
    return _clientAuthEnabled;
  }

  public void setClientAuthEnabled(boolean clientAuthEnabled) {
    _clientAuthEnabled = clientAuthEnabled;
  }

  public String getKeyStoreType() {
    return _keyStoreType;
  }

  public void setKeyStoreType(String keyStoreType) {
    _keyStoreType = keyStoreType;
  }

  public String getKeyStorePath() {
    return _keyStorePath;
  }

  public void setKeyStorePath(String keyStorePath) {
    _keyStorePath = keyStorePath;
  }

  public String getKeyStorePassword() {
    return _keyStorePassword;
  }

  public void setKeyStorePassword(String keyStorePassword) {
    _keyStorePassword = keyStorePassword;
  }

  public String getTrustStoreType() {
    return _trustStoreType;
  }

  public void setTrustStoreType(String trustStoreType) {
    _trustStoreType = trustStoreType;
  }

  public String getTrustStorePath() {
    return _trustStorePath;
  }

  public void setTrustStorePath(String trustStorePath) {
    _trustStorePath = trustStorePath;
  }

  public String getTrustStorePassword() {
    return _trustStorePassword;
  }

  public void setTrustStorePassword(String trustStorePassword) {
    _trustStorePassword = trustStorePassword;
  }

  public String getSslProvider() {
    return _sslProvider;
  }

  public void setSslProvider(String sslProvider) {
    _sslProvider = sslProvider;
  }

  public boolean isCustomized() {
    return StringUtils.isNoneBlank(_keyStorePath) || StringUtils.isNoneBlank(_trustStorePath);
  }

  public boolean isInsecure() {
    return _insecure;
  }

  public void setInsecure(boolean insecure) {
    _insecure = insecure;
  }
}
