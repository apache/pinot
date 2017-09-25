package com.linkedin.thirdeye.auth;

import com.google.common.base.Optional;
import com.linkedin.thirdeye.dashboard.configs.AuthConfiguration;
import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import java.security.Key;
import java.util.Hashtable;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThirdeyeAuthManager implements AuthManager, Authenticator<AuthRequest, PrincipalAuthContext> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdeyeAuthManager.class);

  private static final String LDAP_CONTEXT_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";
  private static final ThreadLocal<PrincipalAuthContext> principalAuthContextThreadLocal = new ThreadLocal<>();

  private final AuthConfiguration authConfiguration;
  private final Key aesKey;

  public ThirdeyeAuthManager(AuthConfiguration authConfiguration) {
    this.authConfiguration = authConfiguration;
    this.aesKey = new SecretKeySpec(Base64.decodeBase64(authConfiguration.getAuthKey()), "AES");
  }

  /**
   *  {@inheritDoc}
   */
  @Override
  public Optional<PrincipalAuthContext> authenticate(AuthRequest authRequest) throws AuthenticationException {
    try {
      if (authRequest.getPrincipal() != null) {
        LOG.info("Authenticating {} via username and password", authRequest.getPrincipal());

        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, LDAP_CONTEXT_FACTORY);
        env.put(Context.PROVIDER_URL, authConfiguration.getLdapUrl());

        if (authConfiguration.getLdapUrl().startsWith("ldaps")) {
          env.put(Context.SECURITY_PROTOCOL, "ssl");
        }
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, authRequest.getPrincipal() + '@' + authConfiguration.getDomainSuffix());
        env.put(Context.SECURITY_CREDENTIALS, authRequest.getPassword());

        // Attempt ldap authentication
        try {
          new InitialDirContext(env).close();
        } catch (NamingException e) {
          throw new AuthenticationException(e);
        }

        PrincipalAuthContext authContext = new PrincipalAuthContext();
        authContext.setPrincipal(authRequest.getPrincipal());

        LOG.info("Successfully authenticated {}", authContext.getPrincipal());
        setCurrentPrincipal(authContext);
        return Optional.of(authContext);
      }

      if (authRequest.getToken() != null) {
        LOG.info("Authenticating token {}", authRequest.getToken());

        PrincipalAuthContext authContext = getPrincipalFromToken(authRequest.getToken());

        LOG.info("Successfully authenticated {}", authContext.getPrincipal());
        setCurrentPrincipal(authContext);
        return Optional.of(authContext);
      }
    } catch (Exception e) {
      throw new AuthenticationException(e);
    }

    LOG.info("Authentication failed for user {}, token {}", authRequest.getPrincipal(), authRequest.getToken());
    return Optional.absent();
  }

  private PrincipalAuthContext getPrincipalFromToken(String token) throws Exception {
    PrincipalAuthContext principalAuthContext = new PrincipalAuthContext();
    String principal = decrypt(token);
    principalAuthContext.setPrincipal(principal);
    return principalAuthContext;
  }

  @Override
  public String buildAuthToken(PrincipalAuthContext principalAuthContext) throws Exception {
    // TODO add salt?
    return encrypt(principalAuthContext.getName());
  }

  private String encrypt(String plain) throws Exception {
    Cipher cipher = Cipher.getInstance("AES"); // not threadsafe
    cipher.init(Cipher.ENCRYPT_MODE, aesKey);
    byte[] encrypted = cipher.doFinal(plain.getBytes());
    return Base64.encodeBase64String(encrypted);
  }

  private String decrypt(String crypted) throws Exception {
    Cipher cipher = Cipher.getInstance("AES");
    cipher.init(Cipher.DECRYPT_MODE, aesKey);
    return new String(cipher.doFinal(Base64.decodeBase64(crypted)));
  }

  private static void setCurrentPrincipal(PrincipalAuthContext principal) {
    principalAuthContextThreadLocal.set(principal);
  }

  /**
   * {@inheritDoc}
   */
  public PrincipalAuthContext getCurrentPrincipal() {
    return principalAuthContextThreadLocal.get();
  }
}
