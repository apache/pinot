package org.apache.pinot.controller.api.listeners;

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

/**
 * Holds TLS configuration settings. Used as a vessel to configure Https Listeners. 
 * 
 * @author Daniel Lavoie
 * @since 0.4.0
 */
public class TlsConfiguration {
  private final String keyStorePath;
  private final String keyStorePassword;
  private final String trustStorePath;
  private final String trustStorePassword;
  private final boolean requiresClientAuth;

  public TlsConfiguration(String keyStorePath, String keyStorePassword, String trustStorePath,
      String trustStorePassword, boolean requiresClientAuth) {
    this.keyStorePath = keyStorePath;
    this.keyStorePassword = keyStorePassword;
    this.trustStorePath = trustStorePath;
    this.trustStorePassword = trustStorePassword;
    this.requiresClientAuth = requiresClientAuth;
  }

  public String getKeyStorePath() {
    return keyStorePath;
  }

  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  public String getTrustStorePath() {
    return trustStorePath;
  }

  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  public boolean isRequiresClientAuth() {
    return requiresClientAuth;
  }
}
