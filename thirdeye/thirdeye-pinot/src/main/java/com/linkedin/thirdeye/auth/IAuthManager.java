package com.linkedin.thirdeye.auth;

public interface IAuthManager {
  PrincipalAuthContext authenticate(String principal, String password) throws Exception;

  String buildAuthToken(PrincipalAuthContext principalAuthContext) throws Exception;

  /**
   * used to get current principal details from session
   * @return
   */
  PrincipalAuthContext getCurrentPrincipal();
}
