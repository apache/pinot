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

import java.util.Collections;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.spi.auth.AuthProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Auth provider for vault tokens, retrieves token from VaultTokenCache.
 */
public class VaultTokenAuthProvider implements AuthProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(VaultTokenAuthProvider.class);

  protected final String _taskToken;
  protected final Map<String, Object> _requestHeaders;

    public VaultTokenAuthProvider() {
        LOGGER.info("VaultTokenAuthProvider: Initializing VaultTokenAuthProvider");

        // Retrieve token from VaultTokenCache (populated by VaultStartupManager)
        _taskToken = VaultTokenCache.getToken();

        if (_taskToken != null && !_taskToken.trim().isEmpty()) {
            // Ensure proper Basic Auth format - add "Basic " prefix if missing
            String authToken = _taskToken.startsWith("Basic ") ? _taskToken : "Basic " + _taskToken;
            _requestHeaders = Collections.singletonMap(HttpHeaders.AUTHORIZATION, authToken);
            LOGGER.info("VaultTokenAuthProvider: Successfully initialized");
        } else {
            LOGGER.error("VaultTokenAuthProvider: Failed to retrieve token from VaultTokenCache");
            _requestHeaders = Collections.emptyMap();
        }
    }

  @Override
  public Map<String, Object> getRequestHeaders() {
    return _requestHeaders;
  }

  @Override
  public String getTaskToken() {
    return _taskToken;
  }
}
