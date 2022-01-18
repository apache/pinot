package org.apache.pinot.common.utils;

import java.util.Base64;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;


public class AuthUtils {
  private AuthUtils() {
  }

  /**
   * Convert a pair of name and password into a http header-compliant base64 encoded token
   *
   * @param name user name
   * @param password password
   * @return base64 encoded basic auth token
   */
  @Nullable
  public static String toBase64AuthToken(String name, String password) {
    if (StringUtils.isBlank(name)) {
      return null;
    }
    String identifier = String.format("%s:%s", name, password);
    return normalizeBase64Token(String.format("Basic %s", Base64.getEncoder().encodeToString(identifier.getBytes())));
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
