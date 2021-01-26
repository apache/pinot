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

import org.apache.commons.lang3.StringUtils;


/**
 * Container object for TLS/SSL configuration of pinot clients and servers (netty, grizzly, etc.)
 */
public class TlsConfig {
  private boolean _clientAuthEnabled;
  private String _keyStorePath;
  private String _keyStorePassword;
  private String _trustStorePath;
  private String _trustStorePassword;

  public boolean isClientAuthEnabled() {
    return _clientAuthEnabled;
  }

  public void setClientAuthEnabled(boolean clientAuthEnabled) {
    _clientAuthEnabled = clientAuthEnabled;
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

  public boolean isCustomized() {
    return StringUtils.isNoneBlank(_keyStorePath) || StringUtils.isNoneBlank(_trustStorePath);
  }
}
