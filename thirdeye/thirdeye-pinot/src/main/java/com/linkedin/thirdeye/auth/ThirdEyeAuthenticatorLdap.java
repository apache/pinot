package com.linkedin.thirdeye.auth;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
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
        LOG.info("Authenticating '{}' via username and password", principalName);

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
        AuthenticationResults authenticationResults = new AuthenticationResults();
        if (principalName.contains("@") || CollectionUtils.isEmpty(domainSuffix)) {
          env.put(Context.SECURITY_PRINCIPAL, principalName);
          AuthenticationResult authenticationResult = authenticate(env);
          authenticationResults.appendAuthenticationResult(authenticationResult);
        } else {
          for (String suffix : domainSuffix) {
            env.put(Context.SECURITY_PRINCIPAL, principalName + '@' + suffix);
            AuthenticationResult authenticationResult = authenticate(env);
            authenticationResults.appendAuthenticationResult(authenticationResult);
            if (authenticationResults.isAuthenticated()) {
              break;
            }
          }
        }

        if (authenticationResults.isAuthenticated()) {
          ThirdEyePrincipal principal = new ThirdEyePrincipal();
          principal.setName(env.get(Context.SECURITY_PRINCIPAL));
          LOG.info("Successfully authenticated {} with LDAP", principalName);
          return Optional.of(principal);
        } else {
          // Failed to authenticate the user; log all error messages.
          List<String> errorMessages = authenticationResults.getMessages();
          for (String errorMessage : errorMessages) {
            LOG.error(errorMessage);
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
   *
   * @return authenticationResults the container for the result.
   */
  private AuthenticationResult authenticate(Hashtable<String, String> authEnv) {
    AuthenticationResult authenticationResult = new AuthenticationResult();
    try {
      new InitialDirContext(authEnv).close();
      authenticationResult.setAuthenticated(true);
      authenticationResult.setMessage(
          String.format("Successfully authenticated '%s' with LDAP", authEnv.get(Context.SECURITY_PRINCIPAL)));
    } catch (NamingException e) {
      authenticationResult.setAuthenticated(false);
      authenticationResult.setMessage(
          String.format("Failed to authenticate '%s' with LDAP: %s", authEnv.get(Context.SECURITY_PRINCIPAL),
              e.getMessage()));
    }
    return authenticationResult;
  }

  /**
   * The authentication result of one authentication try.
   */
  private class AuthenticationResult {
    private boolean isAuthenticated = false;
    private String message = "";

    /**
     * Returns the authentication status.
     *
     * @return true if the last try is succeeded.
     */
    public boolean isAuthenticated() {
      return isAuthenticated;
    }

    /**
     * Sets the authentication status.
     *
     * @param authenticated true if the authentication is succeeded.
     */
    public void setAuthenticated(boolean authenticated) {
      isAuthenticated = authenticated;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = Preconditions.checkNotNull(message);
    }
  }

  /**
   * The container that holds multiple authentication results.
   */
  private class AuthenticationResults {
    private boolean isAuthenticated = false;
    private List<String> messages = new ArrayList<>();

    /**
     * Sets the authentication status to the given result and append the message to the message queue.
     *
     * @param authenticationResult the authentication result to be appended to this container.
     */
    public void appendAuthenticationResult(AuthenticationResult authenticationResult) {
      Preconditions.checkNotNull(authenticationResult);

      isAuthenticated = authenticationResult.isAuthenticated();
      messages.add(Preconditions.checkNotNull(authenticationResult.getMessage()));
    }

    /**
     * Returns the authentication status of the last try.
     *
     * @return true if the last try is succeeded.
     */
    public boolean isAuthenticated() {
      return isAuthenticated;
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
