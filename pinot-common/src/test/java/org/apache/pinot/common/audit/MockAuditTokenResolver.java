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

  private static String _staticReturnValue = DEFAULT_PRINCIPAL;
  private static String _lastAuthHeader;

  @Nullable
  private final String _returnValue;

  /**
   * Constructor for direct instantiation with configurable return value.
   */
  public MockAuditTokenResolver(@Nullable String returnValue) {
    _returnValue = returnValue;
  }

  @Override
  @Nullable
  public AuditUserIdentity resolve(String authHeaderValue) {
    _lastAuthHeader = authHeaderValue;

    // If instantiated with a specific return value, use it
    if (_returnValue != null) {
      return () -> _returnValue;
    }

    // For PluginManager-loaded instances, check prefix and use static config
    if (authHeaderValue.startsWith(TEST_PREFIX)) {
      String principal = _staticReturnValue;
      return () -> principal;
    }
    return null;
  }

  /**
   * Resets static state to defaults.
   */
  public static void reset() {
    _staticReturnValue = DEFAULT_PRINCIPAL;
    _lastAuthHeader = null;
  }

  /**
   * Returns the last auth header value passed to resolve().
   */
  @Nullable
  public static String getLastAuthHeader() {
    return _lastAuthHeader;
  }
}
