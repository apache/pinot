/**
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
package org.apache.pinot.common.utils.tls;

import com.google.common.base.Preconditions;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.exception.GenericSSLContextException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for shared TLS configuration logic
 */
public final class TlsUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TlsUtils.class);

  private static final String CLIENT_AUTH_ENABLED = "client.auth.enabled";
  private static final String KEYSTORE_TYPE = "keystore.type";
  private static final String KEYSTORE_PATH = "keystore.path";
  private static final String KEYSTORE_PASSWORD = "keystore.password";
  private static final String TRUSTSTORE_TYPE = "truststore.type";
  private static final String TRUSTSTORE_PATH = "truststore.path";
  private static final String TRUSTSTORE_PASSWORD = "truststore.password";
  private static final String SSL_PROVIDER = "ssl.provider";
  private static final String SSL_CONTEXT_PROTOCOL = "SSL";

  private static final String FILE_SCHEME = "file";
  private static final String FILE_SCHEME_PREFIX = FILE_SCHEME + "://";
  private static final String FILE_SCHEME_PREFIX_WITHOUT_SLASH = FILE_SCHEME + ":";
  private static final String INSECURE = "insecure";
  private static final String PROTOCOLS = "protocols";

  private static final AtomicReference<SSLContext> SSL_CONTEXT_REF = new AtomicReference<>();
  private static final Set<String> LOGGED_TLS_DIAGNOSTICS_KEYS = ConcurrentHashMap.newKeySet();

  private TlsUtils() {
    // left blank
  }

  /**
   * Extract a TlsConfig instance from a namespaced set of configuration keys.
   *
   * @param pinotConfig pinot configuration
   * @param namespace namespace prefix
   *
   * @return TlsConfig instance
   */
  public static TlsConfig extractTlsConfig(PinotConfiguration pinotConfig, String namespace) {
    return extractTlsConfig(pinotConfig, namespace, new TlsConfig());
  }

  /**
   * Extract a TlsConfig instance from a namespaced set of configuration keys, based on a default config
   *
   * @param pinotConfig pinot configuration
   * @param namespace namespace prefix
   * @param defaultConfig TLS config defaults
   *
   * @return TlsConfig instance
   */
  public static TlsConfig extractTlsConfig(PinotConfiguration pinotConfig, String namespace, TlsConfig defaultConfig) {
    TlsConfig tlsConfig = new TlsConfig(defaultConfig);
    tlsConfig.setClientAuthEnabled(
        pinotConfig.getProperty(key(namespace, CLIENT_AUTH_ENABLED), defaultConfig.isClientAuthEnabled()));
    tlsConfig.setKeyStoreType(
        pinotConfig.getProperty(key(namespace, KEYSTORE_TYPE), defaultConfig.getKeyStoreType()));
    tlsConfig.setKeyStorePath(
        pinotConfig.getProperty(key(namespace, KEYSTORE_PATH), defaultConfig.getKeyStorePath()));
    tlsConfig.setKeyStorePassword(
        pinotConfig.getProperty(key(namespace, KEYSTORE_PASSWORD), defaultConfig.getKeyStorePassword()));
    tlsConfig.setTrustStoreType(
        pinotConfig.getProperty(key(namespace, TRUSTSTORE_TYPE), defaultConfig.getTrustStoreType()));
    tlsConfig.setTrustStorePath(
        pinotConfig.getProperty(key(namespace, TRUSTSTORE_PATH), defaultConfig.getTrustStorePath()));
    tlsConfig.setTrustStorePassword(
        pinotConfig.getProperty(key(namespace, TRUSTSTORE_PASSWORD), defaultConfig.getTrustStorePassword()));
    tlsConfig.setSslProvider(
        pinotConfig.getProperty(key(namespace, SSL_PROVIDER), defaultConfig.getSslProvider()));
    tlsConfig.setInsecure(
        pinotConfig.getProperty(key(namespace, INSECURE), defaultConfig.isInsecure()));

    // Read allowed TLS protocols from config (e.g., "TLSv1.2,TLSv1.3")
    String protocolsConfig = pinotConfig.getProperty(key(namespace, PROTOCOLS));
    if (StringUtils.isNotBlank(protocolsConfig)) {
      String[] protocols = Arrays.stream(protocolsConfig.split(","))
          .map(String::trim)
          .filter(StringUtils::isNotBlank)
          .toArray(String[]::new);
      if (protocols.length > 0) {
        tlsConfig.setAllowedProtocols(protocols);
      } else if (defaultConfig.getAllowedProtocols() != null) {
        tlsConfig.setAllowedProtocols(defaultConfig.getAllowedProtocols());
      }
    } else if (defaultConfig.getAllowedProtocols() != null) {
      tlsConfig.setAllowedProtocols(defaultConfig.getAllowedProtocols());
    }

    return tlsConfig;
  }

  /**
   * Create a KeyManagerFactory instance for a given TlsConfig
   *
   * @param tlsConfig TLS config
   *
   * @return KeyManagerFactory
   */
  public static KeyManagerFactory createKeyManagerFactory(TlsConfig tlsConfig) {
    return createKeyManagerFactory(tlsConfig.getKeyStorePath(), tlsConfig.getKeyStorePassword(),
        tlsConfig.getKeyStoreType());
  }

  /**
   * Create a KeyManagerFactory instance for a given path and key password
   *
   * @param keyStorePath store path
   * @param keyStorePassword password
   * @param keyStoreType keystore type for keystore
   * @return KeyManagerFactory
   */
  public static KeyManagerFactory createKeyManagerFactory(String keyStorePath, String keyStorePassword,
      String keyStoreType) {
    Preconditions.checkNotNull(keyStorePath, "key store path must not be null");
    Preconditions.checkNotNull(keyStorePassword, "key store password must not be null");

    try {
      KeyStore keyStore = KeyStore.getInstance(keyStoreType);
      try (InputStream is = makeKeyOrTrustStoreUrl(keyStorePath).openStream()) {
        keyStore.load(is, keyStorePassword.toCharArray());
      }

      KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());

      return keyManagerFactory;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Could not create key manager factory '%s'", keyStorePath), e);
    }
  }

  /**
   * Create a TrustManagerFactory instance from a given TlsConfig.
   *
   * @param tlsConfig TLS config
   *
   * @return TrustManagerFactory
   */
  public static TrustManagerFactory createTrustManagerFactory(TlsConfig tlsConfig) {
    if (tlsConfig.isInsecure()) {
      return InsecureTrustManagerFactory.INSTANCE;
    } else {
      return createTrustManagerFactory(tlsConfig.getTrustStorePath(), tlsConfig.getTrustStorePassword(),
          tlsConfig.getTrustStoreType());
    }
  }

  /**
   * Create a TrustManagerFactory instance from a given path and key password
   *
   * @param trustStorePath store path
   * @param trustStorePassword password
   * @param trustStoreType keystore type for truststore
   * @return TrustManagerFactory
   */
  public static TrustManagerFactory createTrustManagerFactory(String trustStorePath, String trustStorePassword,
      String trustStoreType) {
    Preconditions.checkNotNull(trustStorePath, "trust store path must not be null");
    Preconditions.checkNotNull(trustStorePassword, "trust store password must not be null");

    try {
      KeyStore keyStore = KeyStore.getInstance(trustStoreType);
      try (InputStream is = makeKeyOrTrustStoreUrl(trustStorePath).openStream()) {
        keyStore.load(is, trustStorePassword.toCharArray());
      }

      TrustManagerFactory trustManagerFactory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(keyStore);

      return trustManagerFactory;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Could not create trust manager factory '%s'", trustStorePath), e);
    }
  }

  /**
   * Installs a default TLS socket factory for all HttpsURLConnection instances based on a given TlsConfig (1 or 2-way)
   *
   * @param tlsConfig TLS config
   */
  public static void installDefaultSSLSocketFactory(TlsConfig tlsConfig) {
    installDefaultSSLSocketFactory(tlsConfig.getKeyStoreType(), tlsConfig.getKeyStorePath(),
        tlsConfig.getKeyStorePassword(), tlsConfig.getTrustStoreType(), tlsConfig.getTrustStorePath(),
        tlsConfig.getTrustStorePassword());
  }

  /**
   * Installs a default TLS socket factory for all HttpsURLConnection instances based on a given set of key and trust
   * store paths and passwords
   * @param keyStoreType keystore type for keystore
   * @param keyStorePath key store path
   * @param keyStorePassword key password
   * @param trustStoreType keystore type for truststore
   * @param trustStorePath trust store path
   * @param trustStorePassword trust password
   */
  public static void installDefaultSSLSocketFactory(String keyStoreType, String keyStorePath, String keyStorePassword,
      String trustStoreType, String trustStorePath, String trustStorePassword) {
    try {
      SecureRandom secureRandom = new SecureRandom();
      SSLContext sc;
      if (keyStorePath == null && trustStorePath == null) {
        // When neither keyStorePath nor trustStorePath is provided, a SSLFactory cannot be created. create SSLContext
        // directly and use the default key manager and trust manager.
        sc = SSLContext.getInstance(SSL_CONTEXT_PROTOCOL);
        sc.init(null, null, secureRandom);
      } else {
        SSLFactory sslFactory =
            RenewableTlsUtils.createSSLFactory(keyStoreType, keyStorePath, keyStorePassword, trustStoreType,
                trustStorePath, trustStorePassword, SSL_CONTEXT_PROTOCOL, secureRandom, true, false);
        if (isKeyOrTrustStorePathNullOrHasFileScheme(keyStorePath) && isKeyOrTrustStorePathNullOrHasFileScheme(
            trustStorePath)) {
          RenewableTlsUtils.enableAutoRenewalFromFileStoreForSSLFactory(sslFactory, keyStoreType, keyStorePath,
              keyStorePassword, trustStoreType, trustStorePath, trustStorePassword, SSL_CONTEXT_PROTOCOL, secureRandom,
              PinotInsecureMode::isPinotInInsecureMode);
        }
        sc = sslFactory.getSslContext();
      }
      // HttpsURLConnection
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
      setSslContext(sc);
      logTlsDiagnosticsOnce("https.default", sc, null, false);
    } catch (GenericSSLContextException | GeneralSecurityException e) {
      throw new IllegalStateException("Could not initialize SSL support", e);
    }
  }

  private static String key(String namespace, String suffix) {
    return namespace + "." + suffix;
  }

  public static URL makeKeyOrTrustStoreUrl(String storePath)
      throws URISyntaxException, MalformedURLException {
    URI inputUri = new URI(storePath);
    if (StringUtils.isBlank(inputUri.getScheme())) {
      if (storePath.startsWith("/")) {
        return new URL(FILE_SCHEME_PREFIX + storePath);
      }
      return new URL(FILE_SCHEME_PREFIX + "./" + storePath);
    }
    return inputUri.toURL();
  }

  /**
   * Get the SSL context, see: {@link SSLContextHolder} for more details.
   * @return the SSL context.
   */
  public static SSLContext getSslContext() {
    return SSLContextHolder.SSL_CONTEXT;
  }

  /**
   * Set the SSL context, see: {@link SSLContextHolder} for more details.
   * @param sslContext the SSL context to be set.
   */
  public static void setSslContext(SSLContext sslContext) {
    if (!SSL_CONTEXT_REF.compareAndSet(null, sslContext)) {
      LOGGER.warn("SSL Context has already been set.");
    }
  }

  /**
   * SSL Context Holder that holds static reference SSL_CONTEXT, reference via {@link SSLContextHolder#SSL_CONTEXT}.
   *
   * this context is set via the {@link TlsUtils#SSL_CONTEXT_REF} which can at most once override the default
   * SSLContext object. The advantage of this design is:
   * <ul>
   *   <li>any override registration is thread safe - it only occur lazily when access SSLContextHolder.SSL_CONTEXT.
   *   <li>mutable until first use.
   *   <li>synchronization, at most once initialisation guaranteed by the classloader
   *   <li>after initialisation, the SSLContext is constant which can drive optimisations like constant folding.
   * </ul>
   */
  private static final class SSLContextHolder {
    static final SSLContext SSL_CONTEXT = SSL_CONTEXT_REF.get() == null ? SSLContexts.createDefault()
        : SSL_CONTEXT_REF.get();
  }

  /**
   * Builds client side SslContext based on a given TlsConfig.
   *
   * @param tlsConfig TLS config
   */
  public static SslContext buildClientContext(TlsConfig tlsConfig) {
    SSLFactory sslFactory =
        RenewableTlsUtils.createSSLFactoryAndEnableAutoRenewalWhenUsingFileStores(
            tlsConfig, PinotInsecureMode::isPinotInInsecureMode);
    SslContextBuilder sslContextBuilder =
        SslContextBuilder.forClient().sslProvider(SslProvider.valueOf(tlsConfig.getSslProvider()));
    sslFactory.getKeyManagerFactory().ifPresent(sslContextBuilder::keyManager);
    sslFactory.getTrustManagerFactory().ifPresent(sslContextBuilder::trustManager);

    // Apply protocol restrictions if configured
    String[] allowedProtocols = tlsConfig.getAllowedProtocols();
    if (allowedProtocols != null && allowedProtocols.length > 0) {
      sslContextBuilder.protocols(allowedProtocols);
      LOGGER.debug("TLS client context restricted to protocols: {}", Arrays.toString(allowedProtocols));
    }

    try {
      warnIfNonJdkProviderConfiguredInternal("netty.client", tlsConfig);
      return sslContextBuilder.build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Builds server side SslContext based on a given TlsConfig.
   *
   * @param tlsConfig TLS config
   */
  public static SslContext buildServerContext(TlsConfig tlsConfig) {
    if (tlsConfig.getKeyStorePath() == null) {
      throw new IllegalArgumentException("Must provide key store path for secured server");
    }
    SSLFactory sslFactory =
        RenewableTlsUtils.createSSLFactoryAndEnableAutoRenewalWhenUsingFileStores(
            tlsConfig, PinotInsecureMode::isPinotInInsecureMode);
    SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(sslFactory.getKeyManagerFactory().get())
        .sslProvider(SslProvider.valueOf(tlsConfig.getSslProvider()));
    sslFactory.getTrustManagerFactory().ifPresent(sslContextBuilder::trustManager);
    if (tlsConfig.isClientAuthEnabled()) {
      sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
    }

    // Apply protocol restrictions if configured
    String[] allowedProtocols = tlsConfig.getAllowedProtocols();
    if (allowedProtocols != null && allowedProtocols.length > 0) {
      sslContextBuilder.protocols(allowedProtocols);
      LOGGER.debug("TLS server context restricted to protocols: {}", Arrays.toString(allowedProtocols));
    }

    try {
      warnIfNonJdkProviderConfiguredInternal("netty.server", tlsConfig);
      return sslContextBuilder.build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static SSLConnectionSocketFactory buildConnectionSocketFactory() {
    return new SSLConnectionSocketFactory(getSslContext());
  }

  /**
   * check if the key store or trust store path is null or has file scheme.
   *
   * @param keyOrTrustStorePath key store or trust store path in String format.
   */
  static boolean isKeyOrTrustStorePathNullOrHasFileScheme(String keyOrTrustStorePath) {
    try {
      return keyOrTrustStorePath == null
          || makeKeyOrTrustStoreUrl(keyOrTrustStorePath).toURI().getScheme().startsWith(FILE_SCHEME);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void warnIfNonJdkProviderConfiguredInternal(String contextName, TlsConfig tlsConfig) {
    // In Platform-FIPS-JDK deployments, you typically want to stay on the JDK TLS stack
    // (JSSE provider selection happens via java.security / JVM configuration).
    String configured = tlsConfig != null ? tlsConfig.getSslProvider() : null;
    if (configured != null) {
      try {
        SslProvider sslProvider = SslProvider.valueOf(configured);
        if (sslProvider != SslProvider.JDK) {
          LOGGER.warn("TLS config for '{}' sets sslProvider='{}'. Platform-FIPS-JDK deployments typically require "
              + "sslProvider='JDK' (avoid OpenSSL).", contextName, configured);
        }
      } catch (Exception e) {
        // If config is invalid, let existing code fail where it parses/builds the context.
      }
    }
  }

  /**
   * Log (once) the JSSE provider/protocol actually used at runtime for the given TLS config.
   * <p>
   * This is intended as a lightweight runtime self-check for Platform-FIPS-JDK deployments: Pinot generally uses the
   * JDK TLS stack (JSSE), and the platform/JDK decides which provider is active via {@code java.security} and other JVM
   * settings. This method helps surface misconfiguration early without enforcing behavior.
   */
  public static void logJsseDiagnosticsOnce(String contextName, SSLFactory sslFactory, TlsConfig tlsConfig) {
    if (sslFactory == null) {
      return;
    }
    try {
      SSLContext sslContext = sslFactory.getSslContext();
      String configuredSslProvider = tlsConfig != null ? tlsConfig.getSslProvider() : null;
      boolean insecure = tlsConfig != null && tlsConfig.isInsecure();
      logTlsDiagnosticsOnce(contextName, sslContext, configuredSslProvider, insecure);
    } catch (Exception e) {
      LOGGER.warn("TLS diagnostics ({}): failed to obtain SSLContext for diagnostics", contextName, e);
    }
  }

  /**
   * Emit a warning when a non-JDK TLS stack is configured.
   */
  public static void warnIfNonJdkProviderConfigured(String contextName, TlsConfig tlsConfig) {
    warnIfNonJdkProviderConfiguredInternal(contextName, tlsConfig);
  }

  private static void logTlsDiagnosticsOnce(String contextName, SSLContext sslContext, String configuredSslProvider,
      boolean insecure) {
    if (sslContext == null) {
      return;
    }
    String providerName = sslContext.getProvider() != null ? sslContext.getProvider().getName() : "null";
    String protocol = sslContext.getProtocol();
    String key = contextName + "|" + providerName + "|" + protocol + "|" + configuredSslProvider + "|" + insecure;
    if (!LOGGED_TLS_DIAGNOSTICS_KEYS.add(key)) {
      return;
    }

    // Basic "what are we actually using at runtime?" visibility.
    LOGGER.info(
        "TLS diagnostics ({}): SSLContext protocol='{}', provider='{}', configuredSslProvider='{}', insecure={}",
        contextName, protocol, providerName, configuredSslProvider, insecure);

    // Heuristic warnings that are helpful for FIPS hardening (without enforcing behavior here).
    if ("SSL".equalsIgnoreCase(protocol)) {
      LOGGER.warn("TLS diagnostics ({}): SSLContext protocol is '{}'. Consider using 'TLS' and enforcing TLSv1.2+ via "
          + "protocol/cipher allowlists for compliance hardening.", contextName, protocol);
    }

    try {
      SSLEngine engine = sslContext.createSSLEngine();
      String[] enabledProtocols = engine.getEnabledProtocols();
      String[] enabledCiphers = engine.getEnabledCipherSuites();
      LOGGER.info("TLS diagnostics ({}): enabledProtocols={}, enabledCipherSuites(count)={}", contextName,
          Arrays.toString(enabledProtocols), enabledCiphers != null ? enabledCiphers.length : 0);

      if (enabledProtocols != null) {
        for (String p : enabledProtocols) {
          if ("TLSv1".equalsIgnoreCase(p) || "TLSv1.1".equalsIgnoreCase(p) || "SSLv3".equalsIgnoreCase(p)
              || "SSLv2Hello".equalsIgnoreCase(p)) {
            LOGGER.warn("TLS diagnostics ({}): enabled protocol '{}' is typically disallowed in modern/FIPS-hardened "
                + "deployments. Consider enforcing TLSv1.2+.", contextName, p);
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("TLS diagnostics ({}): failed to create SSLEngine for diagnostics", contextName, e);
    }
  }
}
