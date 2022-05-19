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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.spi.auth.AuthProvider;


/**
 * Auth provider with dynamic loading support, typically used for rotating tokens such as those injected by kubernetes.
 * UrlAuthProvider will re-read the source on every invocation, so beware of long round-trip times if the source is
 * remote.
 */
public class UrlAuthProvider implements AuthProvider {
  public static final String HEADER = "header";
  public static final String PREFIX = "prefix";
  public static final String URL = "url";

  protected final String _header;
  protected final String _prefix;
  protected final URL _url;

  public UrlAuthProvider(String url) {
    try {
      _header = HttpHeaders.AUTHORIZATION;
      _prefix = "Bearer";
      _url = new URL(url);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public UrlAuthProvider(AuthConfig authConfig) {
    try {
      _header = AuthProviderUtils.getOrDefault(authConfig, HEADER, HttpHeaders.AUTHORIZATION);
      _prefix = AuthProviderUtils.getOrDefault(authConfig, PREFIX, "Bearer");
      _url = new URL(authConfig.getProperties().get(URL).toString());
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public Map<String, Object> getRequestHeaders() {
    return Collections.singletonMap(_header, makeToken());
  }

  @Override
  public String getTaskToken() {
    return makeToken();
  }

  private String makeToken() {
    try {
      String token = IOUtils.toString(_url, StandardCharsets.UTF_8);
      if (token.startsWith(_prefix)) {
        return token;
      }
      return _prefix + " " + token;
    } catch (IOException e) {
      throw new IllegalArgumentException("Could not resolve auth url " + _url, e);
    }
  }
}
