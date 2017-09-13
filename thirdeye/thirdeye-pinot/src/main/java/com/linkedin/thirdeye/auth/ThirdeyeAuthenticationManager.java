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
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import org.apache.commons.codec.binary.Base64;


public class ThirdeyeAuthenticationManager implements IAuthManager, Authenticator<AuthRequest, PrincipalAuthContext> {
  private static final String LDAP_CONTEXT_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";
  private static final ThreadLocal<PrincipalAuthContext> principalAuthContextThreadLocal = new ThreadLocal<>();
  
  private final AuthConfiguration authConfiguration;
  private final Key aesKey;

  public ThirdeyeAuthenticationManager(AuthConfiguration authConfiguration) {
    this.authConfiguration = authConfiguration;
    this.aesKey = new SecretKeySpec(Base64.decodeBase64(authConfiguration.getAuthKey()), "AES");
  }

  public PrincipalAuthContext authenticate(String principal, String password) throws Exception {
    AuthRequest request = new AuthRequest();
    request.setPrincipal(principal);
    request.setPassword(password);
    return authenticate(request).get();
  }

  /**
   *  {@inheritDoc}
   */
  @Override
  public Optional<PrincipalAuthContext> authenticate(AuthRequest authRequest) throws AuthenticationException {
    PrincipalAuthContext authContext = null;
    try {
      if (authRequest.getPrincipal() != null) {
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, LDAP_CONTEXT_FACTORY);
        env.put(Context.PROVIDER_URL, authConfiguration.getLdapUrl());

        if (authConfiguration.getLdapUrl().startsWith("ldaps")) {
          env.put(Context.SECURITY_PROTOCOL, "ssl");
        }
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, authRequest.getPrincipal() + '@' + authConfiguration.getDomainSuffix());
        env.put(Context.SECURITY_CREDENTIALS, authRequest.getPassword());

        // Create the initial context
        DirContext ctx = new InitialDirContext(env);
        boolean result = ctx != null;

        if (ctx != null) {
          ctx.close();
        }

        if (result == true) {
          authContext = new PrincipalAuthContext();
          authContext.setPrincipal(authRequest.getPrincipal());
          // Set the user context
          this.principalAuthContextThreadLocal.set(authContext);
        }
      }
      if (authRequest.getToken() != null) {
        authContext = getPrincipalFromToken(authRequest.getToken());
      }
    } catch (Exception e) {
      throw new AuthenticationException(e);
    }
    setCurrentPrincipal(authContext);
    return Optional.of(authContext);
  }

  private PrincipalAuthContext getPrincipalFromToken(String token) throws Exception {
    PrincipalAuthContext principalAuthContext = new PrincipalAuthContext();
    String principal = decrypt(token);
    principalAuthContext.setPrincipal(principal);
    return principalAuthContext;
  }

  @Override
  public String buildAuthToken(PrincipalAuthContext principalAuthContext) throws Exception {
    return encrypt(principalAuthContext.getName());
  }

  private void setCurrentPrincipal(PrincipalAuthContext principal) {
    this.principalAuthContextThreadLocal.set(principal);
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

  /**
   * {@inheritDoc}
   */
  public PrincipalAuthContext getCurrentPrincipal() {
    return this.principalAuthContextThreadLocal.get();
  }
}
