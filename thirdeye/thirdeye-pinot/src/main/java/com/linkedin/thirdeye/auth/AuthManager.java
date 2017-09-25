package com.linkedin.thirdeye.auth;

import com.google.common.base.Optional;


public interface AuthManager {
  Optional<PrincipalAuthContext> authenticate(AuthRequest authRequest) throws Exception;

  String buildAuthToken(PrincipalAuthContext principalAuthContext) throws Exception;

  /**
   * used to get current principal details from session
   * @return
   */
  PrincipalAuthContext getCurrentPrincipal();
}
