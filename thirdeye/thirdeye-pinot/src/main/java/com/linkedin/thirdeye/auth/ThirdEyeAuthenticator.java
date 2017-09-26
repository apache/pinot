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


public class ThirdEyeAuthenticator implements Authenticator<Credentials, ThirdEyePrincipal> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeAuthenticator.class);

  private static final String LDAP_CONTEXT_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";
  private static final ThreadLocal<ThirdEyePrincipal> principalAuthContextThreadLocal = new ThreadLocal<>();

  private final String domainSuffix;
  private final String ldapUrl;

  public ThirdEyeAuthenticator(String domainSuffix, String ldapUrl) {
    this.domainSuffix = domainSuffix;
    this.ldapUrl = ldapUrl;
  }

  /**
   *  {@inheritDoc}
   */
  @Override
  public Optional<ThirdEyePrincipal> authenticate(Credentials credentials) throws AuthenticationException {
    try {
      if (credentials.getPrincipal() != null) {
        LOG.info("Authenticating {} via username and password", credentials.getPrincipal());

        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, LDAP_CONTEXT_FACTORY);
        env.put(Context.PROVIDER_URL, this.ldapUrl);

        if (this.ldapUrl.startsWith("ldaps")) {
          env.put(Context.SECURITY_PROTOCOL, "ssl");
        }
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, credentials.getPrincipal() + '@' + this.domainSuffix);
        env.put(Context.SECURITY_CREDENTIALS, credentials.getPassword());

        // Attempt ldap authentication
        try {
          new InitialDirContext(env).close();
        } catch (NamingException e) {
          throw new AuthenticationException(e);
        }

        ThirdEyePrincipal principal = new ThirdEyePrincipal();
        principal.setName(credentials.getPrincipal());

        LOG.info("Successfully authenticated {}", credentials.getPrincipal());
        setCurrentPrincipal(principal);
        return Optional.of(principal);
      }

      // TODO add support for authentication via DB token

    } catch (Exception e) {
      throw new AuthenticationException(e);
    }

    LOG.info("Authentication failed for {}", credentials.getPrincipal());
    return Optional.absent();
  }

  private static void setCurrentPrincipal(ThirdEyePrincipal principal) {
    // TODO refactor this, use injectors
    principalAuthContextThreadLocal.set(principal);
  }

  public static ThirdEyePrincipal getCurrentPrincipal() {
    // TODO refactor this, use injectors
    return principalAuthContextThreadLocal.get();
  }
}
