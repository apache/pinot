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

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Utility class to wrap inference of optimal auth provider from component configs.
 */
public final class AuthProviderUtils {
  private AuthProviderUtils() {
    // left blank
  }

  /**
   * Extract an AuthConfig from a pinot configuration subset namespace.
   *
   * @param pinotConfig pinot configuration
   * @param namespace subset namespace
   * @return auth config
   */
  public static AuthConfig extractAuthConfig(PinotConfiguration pinotConfig, String namespace) {
    if (namespace == null) {
      return new AuthConfig(pinotConfig.toMap());
    }
    return new AuthConfig(pinotConfig.subset(namespace).toMap());
  }

  /**
   * Create an AuthProvider after extracting a config from a pinot configuration subset namespace
   * @see AuthProviderUtils#extractAuthConfig(PinotConfiguration, String)
   *
   * @param pinotConfig pinot configuration
   * @param namespace subset namespace
   * @return auth provider
   */
  public static AuthProvider extractAuthProvider(PinotConfiguration pinotConfig, String namespace) {
    return makeAuthProvider(extractAuthConfig(pinotConfig, namespace));
  }

  /**
   * Create auth provider based on the availability of a static token only, if any. This typically applies to task specs
   *
   * @param authToken static auth token
   * @return auth provider
   */
  public static AuthProvider makeAuthProvider(String authToken) {
    if (StringUtils.isBlank(authToken)) {
      return new NullAuthProvider();
    }
    return new StaticTokenAuthProvider(authToken);
  }

  /**
   * Create auth provider based on an auth config. Mimics legacy behavior for static tokens if provided, or dynamic auth
   * providers if additional configs are given.
   *
   * @param authConfig auth config
   * @return auth provider
   */
  public static AuthProvider makeAuthProvider(AuthConfig authConfig) {
    if (authConfig == null) {
      return new NullAuthProvider();
    }

    Object providerClassValue = authConfig.getProperties().get(AuthConfig.PROVIDER_CLASS);
    if (providerClassValue != null) {
      try {
        Class<?> providerClass = Class.forName(providerClassValue.toString());
        Constructor<?> constructor = providerClass.getConstructor(AuthConfig.class);
        return (AuthProvider) constructor.newInstance(authConfig);
      } catch (Exception e) {
        throw new IllegalStateException("Could not create AuthProvider " + providerClassValue, e);
      }
    }

    // mimic legacy behavior for "auth.token" property
    if (authConfig.getProperties().containsKey(StaticTokenAuthProvider.TOKEN)) {
      return new StaticTokenAuthProvider(authConfig);
    }

    if (!authConfig.getProperties().isEmpty()) {
      throw new IllegalArgumentException("Some auth properties defined, but no provider created. Aborting.");
    }

    return new NullAuthProvider();
  }

  /**
   * Convenience helper to convert Map to list of Http Headers
   * @param headers header map
   * @return list of http headers
   */
  public static List<Header> toRequestHeaders(@Nullable Map<String, Object> headers) {
    if (headers == null) {
      return Collections.emptyList();
    }
    return headers.entrySet().stream().filter(entry -> Objects.nonNull(entry.getValue()))
        .map(entry -> new BasicHeader(entry.getKey(), entry.getValue().toString())).collect(Collectors.toList());
  }

  /**
   * Convenience helper to convert an optional authProvider to a list of http headers
   * @param authProvider auth provider
   * @return list of http headers
   */
  public static List<Header> toRequestHeaders(@Nullable AuthProvider authProvider) {
    if (authProvider == null) {
      return Collections.emptyList();
    }
    return toRequestHeaders(authProvider.getRequestHeaders());
  }

  /**
   * Convenience helper to convert an optional authProvider to a static job spec token
   * @param authProvider auth provider
   * @return static token
   */
  public static String toStaticToken(@Nullable AuthProvider authProvider) {
    if (authProvider == null) {
      return null;
    }
    return authProvider.getTaskToken();
  }

  /**
   * Helper to extract string values from complex AuthConfig instance.
   *
   * @param config auth config
   * @param key config key
   * @param defaultValue default value
   * @return config value
   */
  static String getOrDefault(AuthConfig config, String key, String defaultValue) {
    if (config == null || !config.getProperties().containsKey(key)) {
      return defaultValue;
    }
    if (config.getProperties().get(key) instanceof String) {
      return (String) config.getProperties().get(key);
    }
    throw new IllegalArgumentException("Expected String but got " + config.getProperties().get(key).getClass());
  }

  /**
   * Generate an (optional) HTTP Authorization header given an auth config
   *
   * @param authProvider auth provider
   * @return list of headers
   */
  public static List<Header> makeAuthHeaders(AuthProvider authProvider) {
    return toRequestHeaders(authProvider);
  }

  /**
   * Generate an (optional) HTTP Authorization header given an auth config
   *
   * @param authProvider auth provider
   * @return Map of headers
   */
  public static Map<String, String> makeAuthHeadersMap(AuthProvider authProvider) {
    if (authProvider == null) {
      return Collections.emptyMap();
    }
    return authProvider.getRequestHeaders().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
  }

  /**
   * Generate auth token from pass-thru token or generate basic auth from user/password pair
   *
   * @param provider optional provider
   * @param tokenUrl optional token url
   * @param authToken optional pass-thru token
   * @param user optional username
   * @param password optional password
   * @return auth provider, or NullauthProvider if neither pass-thru token nor user info available
   */
  public static AuthProvider makeAuthProvider(@Nullable AuthProvider provider, String tokenUrl, String authToken,
      String user, String password) {
    if (provider != null) {
      return provider;
    }

    if (StringUtils.isNotBlank(tokenUrl)) {
      return new UrlAuthProvider(tokenUrl);
    }

    if (StringUtils.isNotBlank(authToken)) {
      return new StaticTokenAuthProvider(authToken);
    }

    if (StringUtils.isNotBlank(user)) {
      return new StaticTokenAuthProvider(BasicAuthUtils.toBasicAuthToken(user, password));
    }

    return new NullAuthProvider();
  }
}
