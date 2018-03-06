package com.linkedin.thirdeye.auth;

import com.google.common.base.Optional;
import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
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
      String principalName = credentials.getPrincipal();
      if (StringUtils.isBlank(principalName)) {
        LOG.info("Unable to authenticate empty user name.");
        return Optional.absent();
      } else {
        LOG.info("Authenticating {} via username and password", principalName);

        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, LDAP_CONTEXT_FACTORY);
        env.put(Context.PROVIDER_URL, this.ldapUrl);
        if (this.ldapUrl.startsWith("ldaps")) {
          env.put(Context.SECURITY_PROTOCOL, "ssl");
        }
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_CREDENTIALS, credentials.getPassword());

        // Attempt ldap authentication with the following steps:
        // 1. If user's name contains domain name or the system doesn't have any given domain names, then
        //    use the username as is.
        // 2. Else, try out all combinations of username and the given domain names of the system.
        AuthenticationResult authenticationResult = new AuthenticationResult();
        if (principalName.contains("@") || CollectionUtils.isEmpty(domainSuffix)) {
          env.put(Context.SECURITY_PRINCIPAL, principalName);
          authenticate(env, authenticationResult);
        } else {
          for (String suffix : domainSuffix) {
            env.put(Context.SECURITY_PRINCIPAL, principalName + '@' + suffix);
            authenticate(env, authenticationResult);
            if (authenticationResult.isAuthenticated()) {
              break;
            }
          }
        }

        if (authenticationResult.isAuthenticated()) {
          ThirdEyePrincipal principal = new ThirdEyePrincipal();
          principal.setName(env.get(Context.SECURITY_PRINCIPAL));
          LOG.info("Successfully authenticated {} with LDAP", principalName);
          return Optional.of(principal);
        } else {
          // Failed to authenticate the user; log all error messages.
          List<String> errorMessages = authenticationResult.getMessages();
          for (String errorMessage : errorMessages) {
            LOG.error("Unable authenticate user '{}'.\nReason: {}", principalName, errorMessage);
          }
          return Optional.absent();
        }
      }
    } catch (Exception e) {
      throw new AuthenticationException(e);
    }
  }

  /**
   * Tries to authenticate with the given authentication environment and store the result to the given container of
   * authentication results.
   *
   * @param authEnv the table that contains the authentication information.
   * @param authenticationResult the container for the result.
   */
  private void authenticate(Hashtable<String, String> authEnv, AuthenticationResult authenticationResult) {
    try {
      new InitialDirContext(authEnv).close();
      authenticationResult.setAuthenticated(true);
      authenticationResult.addMessage(
          String.format("Successfully authenticated '%s' with LDAP", authEnv.get(Context.SECURITY_PRINCIPAL)));
    } catch (NamingException e) {
      authenticationResult.setAuthenticated(false);
      authenticationResult.addMessage(
          String.format("Failed to authenticate '%s' with LDAP: %s", authEnv.get(Context.SECURITY_PRINCIPAL),
              ExceptionUtils.getStackTrace(e)));
    }
  }

  /**
   * The class holds the authentication result (true or false) and the error messages from previous iterations.
   */
  private class AuthenticationResult {
    private boolean isAuthenticated = false;
    private List<String> messages = new ArrayList<>();

    /**
     * Returns the authentication result of the last try.
     *
     * @return true if the last try is succeeded.
     */
    public boolean isAuthenticated() {
      return isAuthenticated;
    }

    /**
     * Sets the authentication result of the last try.
     *
     * @param authenticated true if the last try is succeeded.
     */
    public void setAuthenticated(boolean authenticated) {
      isAuthenticated = authenticated;
    }

    /**
     * Add the message to the message queue.
     *
     * @param message the message to be added.
     */
    public void addMessage(String message) {
      messages.add(message);
    }

    /**
     * Returns the read-only message queue.
     *
     * @return the read-only message queue.
     */
    public List<String> getMessages() {
      return Collections.unmodifiableList(messages);
    }
  }
}
