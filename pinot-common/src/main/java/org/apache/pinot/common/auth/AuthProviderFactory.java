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
package org.apache.pinot.common.auth;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.auth.vault.VaultStartupManager;
import org.apache.pinot.common.auth.vault.VaultTokenAuthProvider;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory class for creating AuthProvider instances based on configuration.
 * This factory provides a centralized, configuration-driven approach to authentication
 * initialization across all Pinot components (controller, broker, server, minion).
 *
 * <p>Configuration:</p>
 * <ul>
 *   <li><b>auth.provider.type</b>: Specifies the authentication provider type (vault, static, null)</li>
 *   <li><b>vault.*</b>: Vault-specific configuration (when auth.provider.type=vault)</li>
 *   <li><b>auth.token</b>: Static token (when auth.provider.type=static)</li>
 * </ul>
 *
 * <p>Example usage:</p>
 * <pre>
 * PinotConfiguration config = ...;
 * AuthProvider provider = AuthProviderFactory.create(config);
 * </pre>
 *
 * <p>Benefits:</p>
 * <ul>
 *   <li>Single place to configure authentication provider</li>
 *   <li>Singleton Vault initialization (initialized once, reused across components)</li>
 *   <li>Consistent provider behavior across all Pinot components</li>
 *   <li>Easy to extend with new provider types</li>
 * </ul>
 */
public final class AuthProviderFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(AuthProviderFactory.class);

  // Configuration keys
  private static final String AUTH_PROVIDER_TYPE_KEY = "auth.provider.type";
  private static final String AUTH_TOKEN_KEY = "auth.token";

  // Provider type values
  private static final String PROVIDER_TYPE_VAULT = "vault";
  private static final String PROVIDER_TYPE_STATIC = "static";
  private static final String PROVIDER_TYPE_NULL = "null";

  // Known service-specific auth token prefixes for optimized lookup
  private static final Set<String> SERVICE_AUTH_TOKEN_PREFIXES = new HashSet<>(Arrays.asList(
      "pinot.controller",
      "pinot.broker",
      "pinot.server",
      "pinot.minion"
  ));

  // Singleton instance for VaultTokenAuthProvider
  private static volatile VaultTokenAuthProvider _vaultProviderInstance = null;
  private static volatile boolean _vaultInitialized = false;
  private static final Object VAULT_LOCK = new Object();

  private AuthProviderFactory() {
    // Utility class - prevent instantiation
  }

  /**
   * Creates an AuthProvider instance based on the provided configuration.
   * This is the main entry point for all components to obtain an AuthProvider.
   *
   * <p>The factory determines which provider to create based on the following priority:</p>
   * <ol>
   *   <li>If auth.provider.type=vault → VaultTokenAuthProvider (singleton)</li>
   *   <li>If auth.provider.type=static → StaticTokenAuthProvider</li>
   *   <li>If auth.provider.type=null or not specified → NullAuthProvider</li>
   * </ol>
   *
   * <p>For Vault provider, the factory ensures singleton initialization by calling
   * VaultStartupManager.initialize() only once across all components.</p>
   *
   * @param configuration PinotConfiguration containing auth settings
   * @return AuthProvider instance (never null)
   */
  public static AuthProvider create(PinotConfiguration configuration) {
    if (configuration == null) {
      LOGGER.warn("PinotConfiguration is null, returning NullAuthProvider");
      return new NullAuthProvider();
    }

    // Get the auth provider type from configuration
    String providerType = configuration.getProperty(AUTH_PROVIDER_TYPE_KEY);

    if (StringUtils.isBlank(providerType)) {
      LOGGER.info("No auth.provider.type specified, returning NullAuthProvider");
      return new NullAuthProvider();
    }

    LOGGER.info("Creating AuthProvider with type: {}", providerType);

    // Create provider based on type
    switch (providerType.toLowerCase()) {
      case PROVIDER_TYPE_VAULT:
        return createVaultProvider(configuration);

      case PROVIDER_TYPE_STATIC:
        return createStaticProvider(configuration);

      case PROVIDER_TYPE_NULL:
        LOGGER.info("Explicitly configured to use NullAuthProvider");
        return new NullAuthProvider();

      default:
        LOGGER.warn("Unknown auth.provider.type: {}, returning NullAuthProvider", providerType);
        return new NullAuthProvider();
    }
  }

  /**
   * Creates a VaultTokenAuthProvider instance with singleton initialization.
   * Ensures that VaultStartupManager.initialize() is called only once.
   *
   * @param configuration PinotConfiguration containing vault settings
   * @return VaultTokenAuthProvider singleton instance
   */
  private static AuthProvider createVaultProvider(PinotConfiguration configuration) {
    // Double-checked locking for singleton initialization
    if (!_vaultInitialized) {
      synchronized (VAULT_LOCK) {
        if (!_vaultInitialized) {
          LOGGER.info("Initializing Vault authentication (singleton)");
          try {
            // Initialize Vault once - this fetches credentials and caches the token
            VaultStartupManager.initialize(configuration);

            // Create singleton instance
            _vaultProviderInstance = new VaultTokenAuthProvider();
            _vaultInitialized = true;

            LOGGER.info("Vault authentication initialized successfully");
          } catch (Exception e) {
            LOGGER.error("Failed to initialize Vault authentication", e);
            LOGGER.warn("Falling back to NullAuthProvider due to Vault initialization failure");
            return new NullAuthProvider();
          }
        }
      }
    } else {
      LOGGER.info("Vault already initialized, reusing existing VaultTokenAuthProvider instance");
    }

    return _vaultProviderInstance;
  }

  /**
   * Creates a StaticTokenAuthProvider instance.
   * Looks for service-specific auth tokens first (e.g., pinot.controller.segment.fetcher.auth.token),
   * then falls back to generic auth.token if not found.
   *
   * @param configuration PinotConfiguration containing auth tokens
   * @return StaticTokenAuthProvider or NullAuthProvider if token is blank
   */
  private static AuthProvider createStaticProvider(PinotConfiguration configuration) {
    String authToken = null;
    String foundKey = null;

    // Try to find service-specific auth token configuration using known prefixes
    // This is more efficient than iterating through all configuration keys
    for (String prefix : SERVICE_AUTH_TOKEN_PREFIXES) {
      for (String key : configuration.getKeys()) {
        if (key.startsWith(prefix) && key.endsWith(".auth.token")) {
          authToken = configuration.getProperty(key);
          if (StringUtils.isNotBlank(authToken)) {
            foundKey = key;
            break;
          }
        }
      }
      if (foundKey != null) {
        LOGGER.info("Found service-specific auth token: {}, using it for static provider", foundKey);
        break;
      }
    }

    // Fallback to generic auth.token if no service-specific token found
    if (StringUtils.isBlank(authToken)) {
      authToken = configuration.getProperty(AUTH_TOKEN_KEY);
      if (StringUtils.isNotBlank(authToken)) {
        LOGGER.info("Using generic auth.token for static provider");
      }
    }

    if (StringUtils.isBlank(authToken)) {
      LOGGER.warn("auth.provider.type=static but no auth token found (checked service-specific and generic "
          + "auth.token), returning NullAuthProvider");
      return new NullAuthProvider();
    }

    LOGGER.info("Creating StaticTokenAuthProvider with configured token");
    return new StaticTokenAuthProvider(authToken);
  }

  /**
   * Resets the factory state. This is primarily for testing purposes.
   * Should not be called in production code.
   */
  @com.google.common.annotations.VisibleForTesting
  static void reset() {
    synchronized (VAULT_LOCK) {
      _vaultProviderInstance = null;
      _vaultInitialized = false;
      LOGGER.info("AuthProviderFactory reset");
    }
  }

  /**
   * Checks if Vault has been initialized.
   *
   * @return true if Vault is initialized, false otherwise
   */
  public static boolean isVaultInitialized() {
    return _vaultInitialized;
  }
}
