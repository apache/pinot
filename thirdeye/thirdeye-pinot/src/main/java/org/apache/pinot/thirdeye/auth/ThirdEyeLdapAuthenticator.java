/*
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

package org.apache.pinot.thirdeye.auth;

import com.google.common.base.Preconditions;
import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Optional;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.datalayer.bao.SessionManager;
import org.apache.pinot.thirdeye.datalayer.dto.SessionDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThirdEyeLdapAuthenticator implements Authenticator<ThirdEyeCredentials, ThirdEyePrincipal> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeLdapAuthenticator.class);

  private static final String LDAP_CONTEXT_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";

  private final List<String> domainSuffix;
  private final String ldapUrl;
  private final SessionManager sessionDAO;
  private String ldapContextFactory;

  public ThirdEyeLdapAuthenticator(List<String> domainSuffix, String ldapUrl, SessionManager sessionDAO) {
    this.domainSuffix = domainSuffix;
    this.ldapUrl = ldapUrl;
    this.sessionDAO = sessionDAO;
    this.ldapContextFactory = LDAP_CONTEXT_FACTORY;
  }

  public void setInitialContextFactory(String ldapContextFactory) {
    this.ldapContextFactory = Preconditions.checkNotNull(ldapContextFactory);
  }

  /**
   * Attempt ldap authentication with the following steps:
   * 1. If user's name contains domain name or the system doesn't have any given domain names,
   *    then use the username as is.
   * 2. Else, try out all combinations of username and the given domain names of the system.
   */
  private Optional<ThirdEyePrincipal> ldapAuthenticate(String username, String password) {
    LOG.info("Authenticating '{}' via username and password", username);
    Hashtable<String, String> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, ldapContextFactory);
    env.put(Context.PROVIDER_URL, this.ldapUrl);
    if (this.ldapUrl.startsWith("ldaps")) {
      env.put(Context.SECURITY_PROTOCOL, "ssl");
    }
    env.put(Context.SECURITY_AUTHENTICATION, "simple");
    env.put(Context.SECURITY_CREDENTIALS, password);

    AuthenticationResults authenticationResults = new AuthenticationResults();
    if (username.contains("@") || CollectionUtils.isEmpty(domainSuffix)) {
      env.put(Context.SECURITY_PRINCIPAL, username);
      AuthenticationResult authenticationResult = authenticate(env);
      authenticationResults.appendAuthenticationResult(authenticationResult);
    } else {
      for (String suffix : domainSuffix) {
        env.put(Context.SECURITY_PRINCIPAL, username + '@' + suffix);
        AuthenticationResult authenticationResult = authenticate(env);
        authenticationResults.appendAuthenticationResult(authenticationResult);
        if (authenticationResults.isAuthenticated()) {
          break;
        }
      }
    }

    if (authenticationResults.isAuthenticated()) {
      ThirdEyePrincipal principal = new ThirdEyePrincipal(env.get(Context.SECURITY_PRINCIPAL));
      LOG.info("Successfully authenticated {} with LDAP", env.get(Context.SECURITY_PRINCIPAL));
      return Optional.of(principal);
    } else {
      // Failed to authenticate the user; log all error messages.
      List<String> errorMessages = authenticationResults.getMessages();
      for (String errorMessage : errorMessages) {
        LOG.error(errorMessage);
      }
      return Optional.empty();
    }
  }

  /**
   *  {@inheritDoc}
   */
  @Override
  public Optional<ThirdEyePrincipal> authenticate(ThirdEyeCredentials credentials) throws AuthenticationException {
    try {
      if (StringUtils.isNotBlank(credentials.getToken())) {
        SessionDTO sessionDTO = this.sessionDAO.findBySessionKey(credentials.getToken());
        if (sessionDTO != null && System.currentTimeMillis() < sessionDTO.getExpirationTime()) {
          return Optional.of(new ThirdEyePrincipal(credentials.getPrincipal(), credentials.getToken()));
        }
      }

      String username = credentials.getPrincipal();
      String password = credentials.getPassword();

      if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
        LOG.info("Unable to authenticate empty user name/password");
        return Optional.empty();
      } else {
        return ldapAuthenticate(username, password);
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
