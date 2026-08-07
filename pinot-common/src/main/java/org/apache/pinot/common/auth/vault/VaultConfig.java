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
package org.apache.pinot.common.auth.vault;

public class VaultConfig {
  private static final String DEFAULT_RETRY_INTERVAL = "300";
  private static final String DEFAULT_RETRY_LIMIT = "3";

  private String _vaultBaseUrl;
  private String _vaultPath;
  private String _vaultCaCert;
  private String _vaultCert;
  private String _vaultCertKey;
  private String _vaultRetryInterval = DEFAULT_RETRY_INTERVAL;
  private String _vaultRetryLimit = DEFAULT_RETRY_LIMIT;

  public String getVaultBaseUrl() {
    return _vaultBaseUrl;
  }

  public VaultConfig setVaultBaseUrl(String vaultBaseUrl) {
    _vaultBaseUrl = vaultBaseUrl;
    return this;
  }

  public String getVaultPath() {
    return _vaultPath;
  }

  public VaultConfig setVaultPath(String vaultPath) {
    _vaultPath = vaultPath;
    return this;
  }

  public String getVaultCaCert() {
    return _vaultCaCert;
  }

  public VaultConfig setVaultCaCert(String vaultCaCert) {
    _vaultCaCert = vaultCaCert;
    return this;
  }

  public String getVaultCert() {
    return _vaultCert;
  }

  public VaultConfig setVaultCert(String vaultCert) {
    _vaultCert = vaultCert;
    return this;
  }

  public String getVaultCertKey() {
    return _vaultCertKey;
  }

  public VaultConfig setVaultCertKey(String vaultCertKey) {
    _vaultCertKey = vaultCertKey;
    return this;
  }

  public String getVaultRetryInterval() {
    return _vaultRetryInterval;
  }

  public VaultConfig setVaultRetryInterval(String vaultRetryInterval) {
    _vaultRetryInterval = vaultRetryInterval;
    return this;
  }

  public String getVaultRetryLimit() {
    return _vaultRetryLimit;
  }

  public VaultConfig setVaultRetryLimit(String vaultRetryLimit) {
    _vaultRetryLimit = vaultRetryLimit;
    return this;
  }
}
