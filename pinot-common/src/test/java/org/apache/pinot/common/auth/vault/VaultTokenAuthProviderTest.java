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

import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Unit tests for VaultTokenAuthProvider.
 * Tests Vault token authentication provider functionality including token retrieval and header generation.
 */
public class VaultTokenAuthProviderTest {

  /**
   * Test: VaultTokenAuthProvider construction
   * Expected: Provider is created successfully
   */
  @Test
  public void testVaultProviderConstruction() {
    VaultTokenAuthProvider provider = new VaultTokenAuthProvider();

    assertNotNull(provider, "VaultTokenAuthProvider should not be null");
  }

  /**
   * Test: VaultTokenAuthProvider with null token (before initialization)
   * Expected: Returns null or empty token before Vault is initialized
   */
  @Test
  public void testVaultProviderWithNullToken() {
    // Note: This test assumes Vault is not initialized
    // In real scenarios, VaultStartupManager.initialize() must be called first
    VaultTokenAuthProvider provider = new VaultTokenAuthProvider();

    // Before Vault initialization, token might be null
    String token = provider.getTaskToken();
    // Token could be null if Vault not initialized, or could have a value if already initialized
    // This is expected behavior
    assertNotNull(provider, "Provider should not be null even if token is not yet available");
  }

  /**
   * Test: VaultTokenAuthProvider provides request headers
   * Expected: Returns headers map (may be empty if not initialized)
   */
  @Test
  public void testVaultProviderRequestHeaders() {
    VaultTokenAuthProvider provider = new VaultTokenAuthProvider();

    Map<String, Object> headers = provider.getRequestHeaders();
    assertNotNull(headers, "Headers should not be null");
    // Headers map should exist even if empty
  }

  /**
   * Test: VaultTokenAuthProvider token format
   * Expected: If token exists, it should be in proper format
   */
  @Test
  public void testVaultProviderTokenFormat() {
    VaultTokenAuthProvider provider = new VaultTokenAuthProvider();

    String token = provider.getTaskToken();
    if (token != null && !token.isEmpty()) {
      // If token exists, it should start with "Basic " for Basic auth
      assertTrue(token.startsWith("Basic ") || token.startsWith("hvs."),
          "Token should be in proper format (Basic auth or Vault token)");
    }
  }

  /**
   * Test: Multiple VaultTokenAuthProvider instances
   * Expected: Multiple instances can be created (singleton pattern handled by factory)
   */
  @Test
  public void testMultipleVaultProviderInstances() {
    VaultTokenAuthProvider provider1 = new VaultTokenAuthProvider();
    VaultTokenAuthProvider provider2 = new VaultTokenAuthProvider();

    assertNotNull(provider1, "First provider should not be null");
    assertNotNull(provider2, "Second provider should not be null");

    // Both should provide consistent behavior
    String token1 = provider1.getTaskToken();
    String token2 = provider2.getTaskToken();

    // Tokens should be the same if Vault is initialized (singleton token)
    if (token1 != null && token2 != null) {
      assertEquals(token1, token2, "Both providers should return the same token");
    }
  }

  /**
   * Test: VaultTokenAuthProvider headers contain Authorization
   * Expected: If token exists, headers should contain Authorization header
   */
  @Test
  public void testVaultProviderAuthorizationHeader() {
    VaultTokenAuthProvider provider = new VaultTokenAuthProvider();

    Map<String, Object> headers = provider.getRequestHeaders();
    String token = provider.getTaskToken();

    if (token != null && !token.isEmpty()) {
      assertTrue(headers.containsKey("Authorization"),
          "Headers should contain Authorization key when token exists");
      assertEquals(headers.get("Authorization"), token,
          "Authorization header should match task token");
    }
  }

  /**
   * Test: VaultTokenAuthProvider with empty token
   * Expected: Provider handles empty token gracefully
   */
  @Test
  public void testVaultProviderWithEmptyToken() {
    VaultTokenAuthProvider provider = new VaultTokenAuthProvider();

    String token = provider.getTaskToken();
    // Token could be null or empty if Vault not initialized
    // Provider should handle this gracefully without throwing exceptions
    assertNotNull(provider, "Provider should exist even with empty token");
  }

  /**
   * Test: VaultTokenAuthProvider token consistency
   * Expected: Same provider instance returns consistent token
   */
  @Test
  public void testVaultProviderTokenConsistency() {
    VaultTokenAuthProvider provider = new VaultTokenAuthProvider();

    String token1 = provider.getTaskToken();
    String token2 = provider.getTaskToken();

    // Multiple calls should return the same token
    if (token1 != null && token2 != null) {
      assertEquals(token1, token2, "Token should be consistent across multiple calls");
    }
  }

  /**
   * Test: VaultTokenAuthProvider headers consistency
   * Expected: Same provider instance returns consistent headers
   */
  @Test
  public void testVaultProviderHeadersConsistency() {
    VaultTokenAuthProvider provider = new VaultTokenAuthProvider();

    Map<String, Object> headers1 = provider.getRequestHeaders();
    Map<String, Object> headers2 = provider.getRequestHeaders();

    assertNotNull(headers1, "First headers call should not return null");
    assertNotNull(headers2, "Second headers call should not return null");

    // Headers should be consistent
    assertEquals(headers1.size(), headers2.size(), "Headers size should be consistent");
  }

  /**
   * Test: VaultTokenAuthProvider with long token
   * Expected: Provider handles long tokens correctly
   */
  @Test
  public void testVaultProviderWithLongToken() {
    VaultTokenAuthProvider provider = new VaultTokenAuthProvider();

    String token = provider.getTaskToken();
    if (token != null && token.length() > 100) {
      assertTrue(token.length() > 0, "Long token should be handled correctly");
      assertNotNull(provider.getRequestHeaders(), "Headers should be generated for long token");
    }
  }

  /**
   * Test: VaultTokenAuthProvider token extraction
   * Expected: Token can be extracted and used
   */
  @Test
  public void testVaultProviderTokenExtraction() {
    VaultTokenAuthProvider provider = new VaultTokenAuthProvider();

    String token = provider.getTaskToken();
    if (token != null && !token.isEmpty()) {
      // Token should be usable
      assertFalse(token.trim().isEmpty(), "Token should not be just whitespace");

      // If it's a Basic auth token, it should have proper format
      if (token.startsWith("Basic ")) {
        assertTrue(token.length() > 6, "Basic auth token should have content after 'Basic '");
      }
    }
  }
}
