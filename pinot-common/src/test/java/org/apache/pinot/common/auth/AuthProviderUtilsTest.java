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
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link AuthProviderUtils#extractAuthProvider(PinotConfiguration, String)}.
 *
 * <p>These tests specifically guard against a regression where the controller segment-fetcher auth
 * token was silently ignored because callers used the wrong config key prefix. The correct key for
 * the controller is {@code pinot.controller.segment.fetcher.auth.token}; the wrong (legacy) key
 * {@code controller.segment.fetcher.auth.token} must NOT produce an auth header.
 */
public class AuthProviderUtilsTest {

  private static final String CONTROLLER_SEGMENT_FETCHER_AUTH_NAMESPACE =
      "pinot.controller.segment.fetcher.auth";

  @Test
  public void testCorrectKeyProducesAuthHeader() {
    Map<String, Object> props = new HashMap<>();
    props.put("pinot.controller.segment.fetcher.auth.token", "my-secret-token");
    PinotConfiguration config = new PinotConfiguration(props);

    AuthProvider authProvider =
        AuthProviderUtils.extractAuthProvider(config, CONTROLLER_SEGMENT_FETCHER_AUTH_NAMESPACE);

    Map<String, Object> headers = authProvider.getRequestHeaders();
    assertNotNull(headers, "Auth headers must be present when correct key is used");
    assertTrue(headers.containsValue("my-secret-token"),
        "Token value must appear in request headers");
  }

  @Test
  public void testWrongKeyProducesNoAuthHeader() {
    // Old / wrong key that lacks the "pinot." prefix — must NOT be picked up
    Map<String, Object> props = new HashMap<>();
    props.put("controller.segment.fetcher.auth.token", "should-be-ignored");
    PinotConfiguration config = new PinotConfiguration(props);

    AuthProvider authProvider =
        AuthProviderUtils.extractAuthProvider(config, CONTROLLER_SEGMENT_FETCHER_AUTH_NAMESPACE);

    Map<String, Object> headers = authProvider.getRequestHeaders();
    assertTrue(headers == null || headers.isEmpty(),
        "Wrong key must not produce auth headers; got: " + headers);
  }

  @Test
  public void testNullConfigProducesNoAuthHeader() {
    AuthProvider authProvider =
        AuthProviderUtils.extractAuthProvider(null, CONTROLLER_SEGMENT_FETCHER_AUTH_NAMESPACE);

    Map<String, Object> headers = authProvider.getRequestHeaders();
    assertTrue(headers == null || headers.isEmpty(),
        "Null config must produce no auth headers");
  }
}
