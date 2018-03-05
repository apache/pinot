package com.linkedin.thirdeye.auth;

import com.google.common.base.Optional;
import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import java.util.Hashtable;
import java.util.List;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThirdEyeAuthenticatorLdap implements Authenticator<Credentials, ThirdEyePrincipal> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeAuthenticatorLdap.class);

  private static final String LDAP_CONTEXT_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";

  private final List<String> domainSuffix;
  private final String ldapUrl;

  public ThirdEyeAuthenticatorLdap(List<String> domainSuffix, String ldapUrl) {
    this.domainSuffix = domainSuffix;
    this.ldapUrl = ldapUrl;
  }

  /**
   *  {@inheritDoc}
   */
  @Override
  public Optional<ThirdEyePrincipal> authenticate(Credentials credentials) throws AuthenticationException {
    try {
      String simplePrincipalName = credentials.getPrincipal();
      if (StringUtils.isNotBlank(simplePrincipalName)) {
        LOG.info("Authenticating {} via username and password", simplePrincipalName);

        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, LDAP_CONTEXT_FACTORY);
        env.put(Context.PROVIDER_URL, this.ldapUrl);

        if (this.ldapUrl.startsWith("ldaps")) {
          env.put(Context.SECURITY_PROTOCOL, "ssl");
        }
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_CREDENTIALS, credentials.getPassword());

        // Attempt ldap authentication
        boolean isAuthenticated = false;
        if (CollectionUtils.isEmpty(domainSuffix)) {
          env.put(Context.SECURITY_PRINCIPAL, simplePrincipalName);
          isAuthenticated = authenticate(env);
        } else {
          for (String suffix : domainSuffix) {
            env.put(Context.SECURITY_PRINCIPAL, simplePrincipalName + '@' + suffix);
            if (authenticate(env)) {
              isAuthenticated = true;
              break;
            }
          }
        }

        if (isAuthenticated) {
          ThirdEyePrincipal principal = new ThirdEyePrincipal();
          principal.setName(env.get(Context.SECURITY_PRINCIPAL));

          LOG.info("Successfully authenticated {} with LDAP", simplePrincipalName);
          return Optional.of(principal);
        } else {
          LOG.error("Could not authenticate {} with LDAP", simplePrincipalName);
          return Optional.absent();
        }
      } else {
        LOG.info("Unable to authenticate empty user name.");
        return Optional.absent();
      }
    } catch (Exception e) {
      throw new AuthenticationException(e);
    }
  }

  private boolean authenticate(Hashtable<String, String> authEnv) {
    try {
      new InitialDirContext(authEnv).close();
      return true;
    } catch (NamingException e) {
      LOG.warn("Could not authenticate {} with LDAP", authEnv.get(Context.SECURITY_PRINCIPAL), e);
      return false;
    }
  }
}
