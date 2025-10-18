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
package org.apache.pinot.client.admin;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for handling authentication in Pinot admin operations.
 */
public class PinotAdminAuthentication {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotAdminAuthentication.class);

  private PinotAdminAuthentication() {
  }

  public enum AuthType {
    NONE,
    BASIC,
    BEARER,
    CUSTOM
  }

  /**
   * Creates authentication headers for basic authentication.
   *
   * @param username Username
   * @param password Password
   * @return Authentication headers
   */
  public static Map<String, String> createBasicAuthHeaders(String username, String password) {
    String authString = username + ":" + password;
    String encodedAuth = Base64.getEncoder().encodeToString(authString.getBytes());
    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", "Basic " + encodedAuth);
    return headers;
  }

  /**
   * Creates authentication headers for bearer token authentication.
   *
   * @param token Bearer token
   * @return Authentication headers
   */
  public static Map<String, String> createBearerAuthHeaders(String token) {
    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", "Bearer " + token);
    LOGGER.debug("Created bearer authentication headers");
    return headers;
  }

  /**
   * Creates custom authentication headers.
   *
   * @param headers Custom authentication headers
   * @return Authentication headers
   */
  public static Map<String, String> createCustomAuthHeaders(Map<String, String> headers) {
    LOGGER.debug("Created custom authentication headers");
    return new HashMap<>(headers);
  }

  /**
   * Validates authentication configuration.
   *
   * @param authType Authentication type
   * @param authConfig Authentication configuration
   * @throws PinotAdminAuthenticationException If authentication configuration is invalid
   */
  public static void validateAuthConfig(AuthType authType, Map<String, String> authConfig)
      throws PinotAdminAuthenticationException {
    switch (authType) {
      case BASIC:
        if (!authConfig.containsKey("username") || !authConfig.containsKey("password")) {
          throw new PinotAdminAuthenticationException("Basic authentication requires username and password");
        }
        break;
      case BEARER:
        if (!authConfig.containsKey("token")) {
          throw new PinotAdminAuthenticationException("Bearer authentication requires token");
        }
        break;
      case CUSTOM:
        if (authConfig.isEmpty()) {
          throw new PinotAdminAuthenticationException("Custom authentication requires at least one header");
        }
        break;
      case NONE:
      default:
        // No validation needed for none auth type
        break;
    }
  }

  /**
   * Creates authentication headers based on the specified type and configuration.
   *
   * @param authType Authentication type
   * @param authConfig Authentication configuration
   * @return Authentication headers
   * @throws PinotAdminAuthenticationException If authentication configuration is invalid
   */
  public static Map<String, String> createAuthHeaders(AuthType authType, Map<String, String> authConfig)
      throws PinotAdminAuthenticationException {
    validateAuthConfig(authType, authConfig);

    switch (authType) {
      case BASIC:
        return createBasicAuthHeaders(authConfig.get("username"), authConfig.get("password"));
      case BEARER:
        return createBearerAuthHeaders(authConfig.get("token"));
      case CUSTOM:
        return createCustomAuthHeaders(authConfig);
      case NONE:
      default:
        return Map.of();
    }
  }
}
