package com.linkedin.thirdeye.auth;

import com.google.common.base.Optional;
import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThirdEyeAuthenticatorDisabled implements Authenticator<Credentials, ThirdEyePrincipal> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeAuthenticatorDisabled.class);

  /**
   *  {@inheritDoc}
   */
  @Override
  public Optional<ThirdEyePrincipal> authenticate(Credentials credentials) throws AuthenticationException {
    LOG.info("Authentication is disabled. Accepting any credentials for {}.", credentials.getPrincipal());

    ThirdEyePrincipal principal = new ThirdEyePrincipal();
    principal.setName(credentials.getPrincipal());

    return Optional.of(principal);
  }
}
