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

/**
 * Global static cache for storing Vault-generated authentication tokens.
 * This cache holds the Basic Auth token generated from LDAP credentials
 * retrieved from Vault during startup.
 */
public final class VaultTokenCache {

  // Volatile to ensure thread-safe access across multiple components
  private static volatile String _token;

  private VaultTokenCache() {
    // Utility class - prevent instantiation
  }

  /**
   * Sets the authentication token in the global cache.
   *
   * @param t the Base64-encoded Basic Auth token to cache
   */
  public static void setToken(String t) {
    _token = t;
  }

  /**
   * Gets the authentication token from the global cache.
   *
   * @return the cached Base64-encoded Basic Auth token, or null if not set
   */
  public static String getToken() {
    return _token;
  }

  /**
   * Clears the cached token (useful for testing or cleanup).
   */
  public static void clearToken() {
    _token = null;
  }

  /**
   * Checks if a token is currently cached.
   *
   * @return true if a token is available, false otherwise
   */
  public static boolean hasToken() {
    return _token != null && !_token.trim().isEmpty();
  }
}
