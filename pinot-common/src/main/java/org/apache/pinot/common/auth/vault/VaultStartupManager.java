
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages Vault authentication during startup.
 * Fetches LDAP credentials from Vault and generates Basic Auth tokens.
 */
public final class VaultStartupManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(VaultStartupManager.class);

  // JSON field names from Vault response
  private static final String LDAPSEARCH_ID_KEY = "LDAPSEARCH_ID";
  private static final String LDAPSEARCH_PASSWORD_KEY = "LDAPSEARCH_PASSWORD";

  private VaultStartupManager() {
    // Utility class - prevent instantiation
  }

  /**
   * Initialize Vault authentication by fetching credentials and setting up the token cache.
   * Supports multiple prefixes: vault.*, pinot.controller.vault.*, pinot.server.vault.*,
   * pinot.minion.vault.*, pinot.broker.vault.*
   *
   * @param config Configuration containing vault settings
   */
  public static void initialize(PinotConfiguration config) {
    try {
      LOGGER.debug("Checking for Vault configuration...");

      // Define all supported prefixes in priority order (component-specific first, then generic)
      String[] prefixes = {
        "pinot.controller.vault.",
        "pinot.server.vault.",
        "pinot.minion.vault.",
        "pinot.broker.vault.",
        "vault."
      };

      // Check if vault is enabled using priority-based resolution
      String vaultEnabled = getFirstAvailable(config, prefixes, "enabled");

      if (vaultEnabled == null || !Boolean.parseBoolean(vaultEnabled)) {
        LOGGER.info("Vault is disabled or not configured (vault.enabled=false), skipping Vault authentication setup");
        return;
      }

      LOGGER.info("Vault is enabled, checking for required configuration parameters...");

      // Normalize configuration by extracting from all supported prefixes
      Map<String, String> normalizedVaultConfig = normalizeVaultConfiguration(config, prefixes);

      if (normalizedVaultConfig.isEmpty()) {
        LOGGER.info("No Vault configuration parameters found, skipping Vault authentication setup");
        return;
      }

      // Validate required parameters
      String[] requiredParams = {"base-url", "path", "ca-cert", "cert", "cert-key"};
      for (String param : requiredParams) {
        String value = normalizedVaultConfig.get("vault." + param);
        if (value == null || value.trim().isEmpty()) {
          LOGGER.error("Vault is enabled but required parameter is missing or empty: vault.{}", param);
          LOGGER.error("Available vault parameters: {}", normalizedVaultConfig.keySet());
          return; // Skip vault initialization gracefully instead of throwing exception
        }
      }

      LOGGER.info("All required Vault configuration parameters found, initializing Vault authentication...");

      // Load configured vault with normalized configuration
      loadConfiguredVault(normalizedVaultConfig);

      LOGGER.info("Vault authentication initialization completed successfully");
    } catch (Exception e) {
      LOGGER.error("Failed to initialize Vault authentication", e);
      // Instead of throwing exception, log error and continue
      LOGGER.warn("Skipping Vault authentication setup due to initialization failure");
    }
  }

  /**
   * Helper method to get the first available value for a configuration key across multiple prefixes.
   *
   * @param config Configuration object
   * @param prefixes Array of prefixes to try in order
   * @param keyName The key name (without prefix)
   * @return First non-null, non-empty value found, or null if none exists
   */
  private static String getFirstAvailable(PinotConfiguration config, String[] prefixes, String keyName) {
    for (String prefix : prefixes) {
      String fullKey = prefix + keyName;
      String value = config.getProperty(fullKey);
      if (value != null && !value.trim().isEmpty()) {
        LOGGER.debug("Found configuration value for key ending with {}: {}", keyName,
            keyName.toLowerCase().contains("password") || keyName.toLowerCase().contains("key") ? "[REDACTED]"
                : value);
        return value.trim();
      }
    }
    LOGGER.debug("No configuration value found for {} across prefixes: {}", keyName,
        String.join(", ", prefixes));
    return null;
  }

  /**
   * Normalize vault configuration by mapping from namespaced keys to standard vault.* keys.
   *
   * @param config Configuration object
   * @param prefixes Array of prefixes to check
   * @return Map with normalized vault.* keys
   */
  private static Map<String, String> normalizeVaultConfiguration(PinotConfiguration config, String[] prefixes) {
    Map<String, String> normalized = new HashMap<>();
    String[] paramNames = {"base-url", "path", "ca-cert", "cert", "cert-key", "retry-limit", "retry-interval",
        "enabled"};

    for (String paramName : paramNames) {
      String value = getFirstAvailable(config, prefixes, paramName);
      if (value != null) {
        normalized.put("vault." + paramName, value);
        LOGGER.debug("Normalized configuration: vault.{} = {}", paramName,
            paramName.toLowerCase().contains("password") || paramName.toLowerCase().contains("key")
                ? "[REDACTED]" : value);
      }
    }

    LOGGER.info("Normalized vault configuration with {} parameters", normalized.size());
    return normalized;
  }

  /**
   * Load vault configuration using normalized vault.* properties.
   *
   * @param vaultConfigMap Map containing normalized vault.* configuration
   */
  public static void loadConfiguredVault(Map<String, String> vaultConfigMap)
      throws IOException, InterruptedException {

    LOGGER.info("Starting vault configuration loading process...");

    LOGGER.info("Using normalized vault configuration map: {}",
        vaultConfigMap.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getKey().contains("password") || e.getKey().contains("key") ? "[REDACTED]" : e.getValue()
            )));

    // Validation: Ensure vault configuration exists
    if (vaultConfigMap.isEmpty()) {
      LOGGER.error("Vault not loaded, missing configuration properties.");
      throw new IOException("Vault configuration missing: no vault.* properties found in configuration");
    }

    // Build VaultConfig directly from normalized map
    VaultConfig vaultConfig = createVaultConfigFromMap(vaultConfigMap);
    LOGGER.info("Built VaultConfig object: Base URL = {}, Path = {}, Retry Limit = {}, Retry Interval = {}",
        vaultConfig.getVaultBaseUrl(),
        vaultConfig.getVaultPath(),
        vaultConfig.getVaultRetryLimit(),
        vaultConfig.getVaultRetryInterval());

    // Call selfHealingRetry to fetch data from vault
    LOGGER.info("Calling VaultUtil.selfHealingRetry() to fetch data from vault...");
    Map<String, String> vaultData = Collections.unmodifiableMap(VaultUtil.selfHealingRetry(vaultConfig));

    LOGGER.info("Received vault data from selfHealingRetry: {} entries", vaultData != null ? vaultData.size() : 0);
    LOGGER.debug("Vault data keys: {}", vaultData != null ? vaultData.keySet() : "null");
    if (vaultData != null) {
      vaultData.forEach((key, value) -> {
        String logValue = (key.toLowerCase().contains("password") || key.toLowerCase().contains("secret"))
            ? "[REDACTED]" : value;
        LOGGER.debug("Vault data: {} = {}", key, logValue);
      });
    }

    // Validation: Ensure vault data was successfully loaded
    if (vaultData == null || vaultData.isEmpty()) {
      throw new IOException("Failed to load vault data - no data returned from vault");
    }

    // Extract LDAP credentials from vaultData
    String ldapUser = vaultData.get(LDAPSEARCH_ID_KEY);
    String ldapPassword = vaultData.get(LDAPSEARCH_PASSWORD_KEY);

    if (ldapUser == null || ldapPassword == null) {
      throw new IOException("Missing LDAP credentials in vault data: LDAPSEARCH_ID="
          + ldapUser + ", LDAPSEARCH_PASSWORD=" + (ldapPassword != null ? "[REDACTED]" : "null"));
    }

    // Create Basic auth token & save into VaultTokenCache
    // Use char array to handle sensitive credentials data
    char[] userChars = ldapUser.toCharArray();
    char[] passwordChars = ldapPassword.toCharArray();
    char[] credentialsChars = new char[userChars.length + 1 + passwordChars.length];

    System.arraycopy(userChars, 0, credentialsChars, 0, userChars.length);
    credentialsChars[userChars.length] = ':';
    System.arraycopy(passwordChars, 0, credentialsChars, userChars.length + 1, passwordChars.length);

    String token = Base64.getEncoder().encodeToString(
        new String(credentialsChars).getBytes(StandardCharsets.UTF_8));

    // Clear sensitive data from memory
    java.util.Arrays.fill(userChars, ' ');
    java.util.Arrays.fill(passwordChars, ' ');
    java.util.Arrays.fill(credentialsChars, ' ');

    VaultTokenCache.setToken(token);

    LOGGER.info("Successfully loaded vault entries and cached authentication token");
  }

  /**
   * Create VaultConfig from normalized configuration map.
   */
  private static VaultConfig createVaultConfigFromMap(Map<String, String> map)
      throws IOException {

    // Validate required configuration parameters
    String baseUrl = map.get("vault.base-url");
    String path = map.get("vault.path");

    if (baseUrl == null || baseUrl.trim().isEmpty()) {
      throw new IOException("Missing required vault configuration: 'vault.base-url' cannot be null or empty");
    }

    if (path == null || path.trim().isEmpty()) {
      throw new IOException("Missing required vault configuration: 'vault.path' cannot be null or empty");
    }

    VaultConfig vaultConfig = new VaultConfig();
    vaultConfig.setVaultBaseUrl(baseUrl.trim());
    vaultConfig.setVaultPath(path.trim());
    vaultConfig.setVaultCaCert(map.get("vault.ca-cert"));
    vaultConfig.setVaultCert(map.get("vault.cert"));
    vaultConfig.setVaultCertKey(map.get("vault.cert-key"));
    vaultConfig.setVaultRetryLimit(map.getOrDefault("vault.retry-limit", "3"));
    vaultConfig.setVaultRetryInterval(map.getOrDefault("vault.retry-interval", "1000"));

    LOGGER.info("Created VaultConfig successfully");

    return vaultConfig;
  }
}
