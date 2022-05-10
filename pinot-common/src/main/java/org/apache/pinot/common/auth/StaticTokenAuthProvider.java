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

import java.util.Collections;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.spi.auth.AuthProvider;


/**
 * Auth provider for static client tokens, typically used for job specs or when mimicking legacy behavior.
 */
public class StaticTokenAuthProvider implements AuthProvider {
  public static final String HEADER = "header";
  public static final String PREFIX = "prefix";
  public static final String TOKEN = "token";

  protected final String _taskToken;
  protected final Map<String, Object> _requestHeaders;

  public StaticTokenAuthProvider(String token) {
    _taskToken = token;
    _requestHeaders = Collections.singletonMap(HttpHeaders.AUTHORIZATION, token);
  }

  public StaticTokenAuthProvider(AuthConfig authConfig) {
    String header = AuthProviderUtils.getOrDefault(authConfig, HEADER, HttpHeaders.AUTHORIZATION);
    String prefix = AuthProviderUtils.getOrDefault(authConfig, PREFIX, "Basic");
    String userToken = authConfig.getProperties().get(TOKEN).toString();

    _taskToken = makeToken(prefix, userToken);
    _requestHeaders = Collections.singletonMap(header, _taskToken);
  }

  @Override
  public Map<String, Object> getRequestHeaders() {
    return _requestHeaders;
  }

  @Override
  public String getTaskToken() {
    return _taskToken;
  }

  private static String makeToken(String prefix, String token) {
    if (token.startsWith(prefix)) {
      return token;
    }
    return prefix + " " + token;
  }
}
