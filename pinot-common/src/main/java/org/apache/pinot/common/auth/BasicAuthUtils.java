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

import java.util.Base64;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.BcryptUtils;


/**
 * Utility for configuring basic auth and parsing related http tokens
 */
public final class BasicAuthUtils {
  private static final String ALL = "*";

  private BasicAuthUtils() {
    // left blank
  }

  /**
   * Convert a pair of name and password into a http header-compliant base64 encoded token
   *
   * @param name user name
   * @param password password
   * @return base64 encoded basic auth token
   */
  @Nullable
  public static String toBasicAuthToken(String name, String password) {
    if (StringUtils.isBlank(name)) {
      return null;
    }
    String identifier = String.format("%s:%s", name, password);
    return normalizeBase64Token(String.format("Basic %s", Base64.getEncoder().encodeToString(identifier.getBytes())));
  }

  public static String decodeBasicAuthToken(String auth) {
    if (StringUtils.isBlank(auth)) {
      return null;
    }
    String replacedAuth = StringUtils.replace(auth, "Basic ", "");
    byte[] decodedBytes = Base64.getDecoder().decode(replacedAuth);
    String decodedString = new String(decodedBytes);
    return decodedString;
  }

  public static String extractUsername(String auth) {
    String decodedString = decodeBasicAuthToken(auth);
    return StringUtils.split(decodedString, ":")[0];
  }

  public static String extractPassword(String auth) {
    String decodedString = decodeBasicAuthToken(auth);
    return StringUtils.split(decodedString, ":")[1];
  }

  /**
   * Convert http header-compliant base64 encoded token to password-encrypted base64 token
   *
   * @param auth http base64 token
   * @return base64 encoded basic auth token
   */
  public static String toEncryptBasicAuthToken(String auth) {
    if (StringUtils.isBlank(auth)) {
      return null;
    }
    String replacedAuth = StringUtils.replace(auth, "Basic ", "");
    byte[] decodedBytes = Base64.getDecoder().decode(replacedAuth);
    String decodedString = new String(decodedBytes);
    String[] cretential = StringUtils.split(decodedString, ":");
    String rawUsername = cretential[0];
    String rawPassword = cretential[1];
    String encryptedPassword = BcryptUtils.encrypt(rawPassword);
    return toBasicAuthToken(rawUsername, encryptedPassword);
  }

  /**
   * Normalize a base64 encoded auth token by stripping redundant padding (spaces, '=')
   *
   * @param token base64 encoded auth token
   * @return normalized auth token
   */
  @Nullable
  public static String normalizeBase64Token(String token) {
    if (token == null) {
      return null;
    }
    return StringUtils.remove(token.trim(), '=');
  }
}
