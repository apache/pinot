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
package org.apache.pinot.common.audit;

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.spi.audit.AuditTokenResolver;
import org.apache.pinot.spi.audit.AuditUserIdentity;


/**
 * Mock implementation of AuditTokenResolver for unit testing.
 * <p>
 * Supports two modes:
 * <ul>
 *   <li>Direct instantiation with configurable return value</li>
 *   <li>PluginManager loading (no-arg constructor) with static configuration</li>
 * </ul>
 */
public class MockAuditTokenResolver implements AuditTokenResolver {

  private static final String TEST_PREFIX = "Bearer test-";
  private static final String DEFAULT_PRINCIPAL = "mock-resolved-user";

  private static final class MockConfig {
    final String _returnValue;
    final String _lastAuthHeader;

    MockConfig(@Nullable String returnValue, @Nullable String lastAuthHeader) {
      _returnValue = returnValue;
      _lastAuthHeader = lastAuthHeader;
    }
  }

  private static final AtomicReference<MockConfig> STATIC_CONFIG =
      new AtomicReference<>(new MockConfig(DEFAULT_PRINCIPAL, null));

  @Nullable
  private final String _returnValue;

  /**
   * No-arg constructor for PluginManager loading.
   * Uses static configuration via {@link #setStaticReturnValue(String)}.
   */
  public MockAuditTokenResolver() {
    _returnValue = null;
  }

  /**
   * Constructor for direct instantiation with configurable return value.
   */
  public MockAuditTokenResolver(@Nullable String returnValue) {
    _returnValue = returnValue;
  }

  @Override
  @Nullable
  public AuditUserIdentity resolve(String authHeaderValue) {
    // Record the auth header atomically before any return path
    MockConfig config = STATIC_CONFIG.updateAndGet(c -> new MockConfig(c._returnValue, authHeaderValue));

    // If instantiated with a specific return value, use it
    if (_returnValue != null) {
      return () -> _returnValue;
    }

    // For PluginManager-loaded instances, check prefix and use static config
    if (authHeaderValue != null && authHeaderValue.startsWith(TEST_PREFIX)) {
      String principal = config._returnValue;
      return () -> principal;
    }
    return null;
  }

  /**
   * Sets the return value for PluginManager-loaded instances.
   */
  public static void setStaticReturnValue(@Nullable String value) {
    STATIC_CONFIG.updateAndGet(c -> new MockConfig(value, c._lastAuthHeader));
  }

  /**
   * Resets static state to defaults.
   */
  public static void reset() {
    STATIC_CONFIG.set(new MockConfig(DEFAULT_PRINCIPAL, null));
  }

  /**
   * Returns the last auth header value passed to resolve().
   */
  @Nullable
  public static String getLastAuthHeader() {
    return STATIC_CONFIG.get()._lastAuthHeader;
  }
}
