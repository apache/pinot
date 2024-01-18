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
package org.apache.pinot.common.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.exception.GenericSSLContextException;
import org.apache.commons.lang.StringUtils;
import org.apache.http.ssl.SSLContexts;
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

  private static final String FILE_SCHEME = "file";
  private static final String FILE_SCHEME_PREFIX = FILE_SCHEME + "://";
  private static final String FILE_SCHEME_PREFIX_WITHOUT_SLASH = FILE_SCHEME + ":";

  private static final AtomicReference<SSLContext> SSL_CONTEXT_REF = new AtomicReference<>();

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
    return createTrustManagerFactory(tlsConfig.getTrustStorePath(), tlsConfig.getTrustStorePassword(),
        tlsConfig.getTrustStoreType());
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
      SSLFactory sslFactory = createSSLFactory(keyStoreType, keyStorePath, keyStorePassword,
          trustStoreType, trustStorePath, trustStorePassword,
          "SSL", new java.security.SecureRandom());
      // HttpsURLConnection
      HttpsURLConnection.setDefaultSSLSocketFactory(sslFactory.getSslSocketFactory());
      setSslContext(sslFactory.getSslContext());
    } catch (GenericSSLContextException e) {
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
    SSLFactory sslFactory = createSSLFactory(tlsConfig);
    SslContextBuilder sslContextBuilder =
        SslContextBuilder.forClient().sslProvider(SslProvider.valueOf(tlsConfig.getSslProvider()));
    sslFactory.getKeyManagerFactory().ifPresent(sslContextBuilder::keyManager);
    sslFactory.getTrustManagerFactory().ifPresent(sslContextBuilder::trustManager);
    try {
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
    SSLFactory sslFactory = createSSLFactory(tlsConfig);
    SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(sslFactory.getKeyManagerFactory().get())
        .sslProvider(SslProvider.valueOf(tlsConfig.getSslProvider()));
    sslFactory.getTrustManagerFactory().ifPresent(sslContextBuilder::trustManager);
    if (tlsConfig.isClientAuthEnabled()) {
      sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
    }
    try {
      return sslContextBuilder.build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a {@link SSLFactory} instance with identity material and trust material swappable for a given TlsConfig
   * @param tlsConfig {@link TlsConfig}
   * @return a {@link SSLFactory} instance with identity material and trust material swappable
   */
  public static SSLFactory createSSLFactory(TlsConfig tlsConfig) {
    return createSSLFactory(
        tlsConfig.getKeyStoreType(), tlsConfig.getKeyStorePath(), tlsConfig.getKeyStorePassword(),
        tlsConfig.getTrustStoreType(), tlsConfig.getTrustStorePath(), tlsConfig.getTrustStorePassword(),
        null, null);
  }

  @VisibleForTesting
  static SSLFactory createSSLFactory(
      String keyStoreType, String keyStorePath, String keyStorePassword,
      String trustStoreType, String trustStorePath, String trustStorePassword,
      String sslContextProtocol, SecureRandom secureRandom) {
    try {
      SSLFactory.Builder sslFactoryBuilder = SSLFactory.builder();
      InputStream keyStoreStream = null;
      InputStream trustStoreStream = null;
      if (keyStorePath != null) {
        Preconditions.checkNotNull(keyStorePassword, "key store password must not be null");
        keyStoreStream = makeKeyOrTrustStoreUrl(keyStorePath).openStream();
        sslFactoryBuilder
            .withSwappableIdentityMaterial()
            .withIdentityMaterial(keyStoreStream, keyStorePassword.toCharArray(), keyStoreType);
      }
      if (trustStorePath != null) {
        Preconditions.checkNotNull(trustStorePassword, "trust store password must not be null");
        trustStoreStream = makeKeyOrTrustStoreUrl(trustStorePath).openStream();
        sslFactoryBuilder
            .withSwappableTrustMaterial()
            .withTrustMaterial(trustStoreStream, trustStorePassword.toCharArray(), trustStoreType);
      }
      if (sslContextProtocol != null) {
        sslFactoryBuilder.withSslContextAlgorithm(sslContextProtocol);
      }
      if (secureRandom != null) {
        sslFactoryBuilder.withSecureRandom(secureRandom);
      }
      SSLFactory sslFactory = sslFactoryBuilder.build();
      if (keyStoreStream != null) {
        keyStoreStream.close();
      }
      if (trustStoreStream != null) {
        trustStoreStream.close();
      }
      return sslFactory;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
