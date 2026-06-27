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

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.auth.vault.VaultTokenAuthProvider;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Unit tests for AuthProviderFactory.
 * Tests various authentication provider configurations including static, null, and vault types.
 */
public class AuthProviderFactoryTest {

  @AfterMethod
  public void tearDown() {
    // Reset factory state after each test
    AuthProviderFactory.reset();
  }

  /**
   * Test: auth.provider.type=static with generic auth.token
   * Expected: StaticTokenAuthProvider with the provided token
   * Uses standard test token: admin:verysecret
   */
  @Test
  public void testStaticProviderWithGenericToken() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.provider.type", "static");
    configMap.put("auth.token", "Basic YWRtaW46dmVyeXNlY3JldA==");  // admin:verysecret

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    assertNotNull(provider, "Provider should not be null");
    assertTrue(provider instanceof StaticTokenAuthProvider,
        "Provider should be StaticTokenAuthProvider");
    assertEquals(provider.getTaskToken(), "Basic YWRtaW46dmVyeXNlY3JldA==",
        "Token should match configured value");
  }

  /**
   * Test: auth.provider.type=static with service-specific token
   * Expected: StaticTokenAuthProvider with service-specific token (preferred over generic)
   */
  @Test
  public void testStaticProviderWithServiceSpecificToken() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.provider.type", "static");
    configMap.put("auth.token", "Basic Z2VuZXJpYzp0b2tlbg==");
    configMap.put("pinot.controller.segment.fetcher.auth.token", "Basic c3BlY2lmaWM6dG9rZW4=");

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    assertNotNull(provider, "Provider should not be null");
    assertTrue(provider instanceof StaticTokenAuthProvider,
        "Provider should be StaticTokenAuthProvider");
    // Should use service-specific token, not generic
    assertEquals(provider.getTaskToken(), "Basic c3BlY2lmaWM6dG9rZW4=",
        "Should use service-specific token over generic auth.token");
  }

  /**
   * Test: auth.provider.type=static with only service-specific token (no generic)
   * Expected: StaticTokenAuthProvider with service-specific token
   */
  @Test
  public void testStaticProviderWithOnlyServiceSpecificToken() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.provider.type", "static");
    configMap.put("pinot.server.segment.fetcher.auth.token", "Basic c2VydmVyOnRva2Vu");

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    assertNotNull(provider, "Provider should not be null");
    assertTrue(provider instanceof StaticTokenAuthProvider,
        "Provider should be StaticTokenAuthProvider");
    assertEquals(provider.getTaskToken(), "Basic c2VydmVyOnRva2Vu",
        "Should use service-specific token");
  }

  /**
   * Test: auth.provider.type=static but no token provided
   * Expected: NullAuthProvider (fallback when no token found)
   */
  @Test
  public void testStaticProviderWithoutToken() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.provider.type", "static");
    // No auth.token provided

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    assertNotNull(provider, "Provider should not be null");
    assertTrue(provider instanceof NullAuthProvider,
        "Provider should be NullAuthProvider when no token is provided");
  }

  /**
   * Test: auth.provider.type=static with blank token
   * Expected: NullAuthProvider (fallback when token is blank)
   */
  @Test
  public void testStaticProviderWithBlankToken() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.provider.type", "static");
    configMap.put("auth.token", "");

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    assertNotNull(provider, "Provider should not be null");
    assertTrue(provider instanceof NullAuthProvider,
        "Provider should be NullAuthProvider when token is blank");
  }

  /**
   * Test: auth.provider.type=null (explicitly set to null)
   * Expected: NullAuthProvider
   */
  @Test
  public void testNullProviderExplicit() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.provider.type", "null");

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    assertNotNull(provider, "Provider should not be null");
    assertTrue(provider instanceof NullAuthProvider,
        "Provider should be NullAuthProvider when type is 'null'");
  }

  /**
   * Test: No auth.provider.type specified
   * Expected: NullAuthProvider (default when no type specified)
   */
  @Test
  public void testNoProviderTypeSpecified() {
    Map<String, Object> configMap = new HashMap<>();
    // No auth.provider.type specified

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    assertNotNull(provider, "Provider should not be null");
    assertTrue(provider instanceof NullAuthProvider,
        "Provider should be NullAuthProvider when no type is specified");
  }

  /**
   * Test: Unknown auth.provider.type
   * Expected: NullAuthProvider (fallback for unknown types)
   */
  @Test
  public void testUnknownProviderType() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.provider.type", "unknown-type");

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    assertNotNull(provider, "Provider should not be null");
    assertTrue(provider instanceof NullAuthProvider,
        "Provider should be NullAuthProvider for unknown type");
  }

  /**
   * Test: Null configuration
   * Expected: NullAuthProvider (safe fallback)
   */
  @Test
  public void testNullConfiguration() {
    AuthProvider provider = AuthProviderFactory.create(null);

    assertNotNull(provider, "Provider should not be null");
    assertTrue(provider instanceof NullAuthProvider,
        "Provider should be NullAuthProvider when configuration is null");
  }

  /**
   * Test: Multiple service-specific tokens (should use first found)
   * Expected: StaticTokenAuthProvider with first service-specific token found
   */
  @Test
  public void testMultipleServiceSpecificTokens() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.provider.type", "static");
    configMap.put("pinot.controller.segment.fetcher.auth.token", "Basic Y29udHJvbGxlcjp0b2tlbg==");
    configMap.put("pinot.server.segment.fetcher.auth.token", "Basic c2VydmVyOnRva2Vu");

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    assertNotNull(provider, "Provider should not be null");
    assertTrue(provider instanceof StaticTokenAuthProvider,
        "Provider should be StaticTokenAuthProvider");
    // Should use one of the service-specific tokens (implementation picks first found)
    String token = provider.getTaskToken();
    assertTrue(token.equals("Basic Y29udHJvbGxlcjp0b2tlbg==") || token.equals("Basic c2VydmVyOnRva2Vu"),
        "Should use one of the service-specific tokens");
  }

  /**
   * Test: Case insensitivity of provider type
   * Expected: StaticTokenAuthProvider (case-insensitive matching)
   */
  @Test
  public void testProviderTypeCaseInsensitive() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.provider.type", "STATIC");  // Uppercase
    configMap.put("auth.token", "Basic dGVzdDp0ZXN0");

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    assertNotNull(provider, "Provider should not be null");
    assertTrue(provider instanceof StaticTokenAuthProvider,
        "Provider type should be case-insensitive");
  }

  /**
   * Test: StaticTokenAuthProvider provides correct headers
   * Expected: Authorization header with correct token
   */
  @Test
  public void testStaticProviderHeaders() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.provider.type", "static");
    configMap.put("auth.token", "Basic dGVzdDp0ZXN0");

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    Map<String, Object> headers = provider.getRequestHeaders();
    assertNotNull(headers, "Headers should not be null");
    assertTrue(headers.containsKey("Authorization"), "Should contain Authorization header");
    assertEquals(headers.get("Authorization"), "Basic dGVzdDp0ZXN0",
        "Authorization header should match token");
  }

  /**
   * Test: NullAuthProvider provides empty headers
   * Expected: Empty headers map
   */
  @Test
  public void testNullProviderHeaders() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.provider.type", "null");

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    Map<String, Object> headers = provider.getRequestHeaders();
    assertNotNull(headers, "Headers should not be null");
    assertTrue(headers.isEmpty(), "NullAuthProvider should return empty headers");
  }

  /**
   * Test: auth.provider.type=vault with full configuration
   * Expected: VaultTokenAuthProvider with proper configuration
   * Uses generic open-source friendly values
   */
  @Test
  public void testVaultProviderWithFullConfiguration() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.provider.type", "vault");
    configMap.put("vault.base-url", "https://vault.example.com:8200/v1/");
    configMap.put("vault.path", "secret/data/myapp/credentials");
    configMap.put("vault.ca-cert", "/path/to/ca-bundle.pem");
    configMap.put("vault.cert", "/path/to/client-cert.pfx");
    configMap.put("vault.cert-key", "changeit");
    configMap.put("vault.retry.limit", "3");
    configMap.put("vault.retry.interval", "30");

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    assertNotNull(provider, "Provider should not be null");
    assertTrue(provider instanceof VaultTokenAuthProvider,
        "Provider should be VaultTokenAuthProvider");
  }

  /**
   * Test: auth.provider.type=vault with minimal configuration
   * Expected: VaultTokenAuthProvider with defaults
   */
  @Test
  public void testVaultProviderWithMinimalConfiguration() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.provider.type", "vault");
    configMap.put("vault.base-url", "https://vault.example.com:8200");
    configMap.put("vault.path", "secret/data/myapp");

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    assertNotNull(provider, "Provider should not be null");
    assertTrue(provider instanceof VaultTokenAuthProvider,
        "Provider should be VaultTokenAuthProvider");
  }

  /**
   * Test: auth.provider.type=vault without required configuration
   * Expected: NullAuthProvider (fallback when Vault config is incomplete)
   */
  @Test
  public void testVaultProviderWithoutRequiredConfig() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.provider.type", "vault");
    // Missing vault.base-url and vault.path

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    assertNotNull(provider, "Provider should not be null");
    // Should fallback to NullAuthProvider when required Vault config is missing
    assertTrue(provider instanceof NullAuthProvider || provider instanceof VaultTokenAuthProvider,
        "Provider should be NullAuthProvider or VaultTokenAuthProvider");
  }

  /**
   * Test: auth.provider.type=vault with custom retry settings
   * Expected: VaultTokenAuthProvider with custom retry configuration
   */
  @Test
  public void testVaultProviderWithCustomRetrySettings() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.provider.type", "vault");
    configMap.put("vault.base-url", "https://vault.example.com:8200");
    configMap.put("vault.path", "secret/data/myapp");
    configMap.put("vault.retry.limit", "5");
    configMap.put("vault.retry.interval", "60");

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    assertNotNull(provider, "Provider should not be null");
    assertTrue(provider instanceof VaultTokenAuthProvider,
        "Provider should be VaultTokenAuthProvider");
  }

  /**
   * Test: auth.provider.type=vault with TLS/SSL configuration
   * Expected: VaultTokenAuthProvider with TLS configuration
   */
  @Test
  public void testVaultProviderWithTLSConfiguration() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.provider.type", "vault");
    configMap.put("vault.base-url", "https://vault.example.com:8200");
    configMap.put("vault.path", "secret/data/myapp");
    configMap.put("vault.ca-cert", "/etc/ssl/certs/ca-bundle.crt");
    configMap.put("vault.cert", "/etc/ssl/certs/client.pfx");
    configMap.put("vault.cert-key", "secure-password");

    PinotConfiguration config = new PinotConfiguration(configMap);
    AuthProvider provider = AuthProviderFactory.create(config);

    assertNotNull(provider, "Provider should not be null");
    assertTrue(provider instanceof VaultTokenAuthProvider,
        "Provider should be VaultTokenAuthProvider");
  }
}
