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
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.exception.GenericSSLContextException;
import nl.altindag.ssl.keymanager.HotSwappableX509ExtendedKeyManager;
import nl.altindag.ssl.trustmanager.HotSwappableX509ExtendedTrustManager;
import nl.altindag.ssl.util.SSLFactoryUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.ssl.SSLContexts;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
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
      SecureRandom secureRandom = new SecureRandom();
      SSLFactory sslFactory = createSSLFactory(keyStoreType, keyStorePath, keyStorePassword,
          trustStoreType, trustStorePath, trustStorePassword,
          "SSL", secureRandom, true);
      if (isKeyOrTrustStorePathNullOrHasFileScheme(keyStorePath)
          && isKeyOrTrustStorePathNullOrHasFileScheme(trustStorePath)) {
        enableAutoRenewalFromFileStoreForSSLFactory(sslFactory, keyStoreType, keyStorePath, keyStorePassword,
            trustStoreType, trustStorePath, trustStorePassword, "SSL", secureRandom);
      }
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
    if (isKeyOrTrustStorePathNullOrHasFileScheme(tlsConfig.getKeyStorePath())
        && isKeyOrTrustStorePathNullOrHasFileScheme(tlsConfig.getTrustStorePath())) {
      enableAutoRenewalFromFileStoreForSSLFactory(sslFactory, tlsConfig);
    }
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
    if (isKeyOrTrustStorePathNullOrHasFileScheme(tlsConfig.getKeyStorePath())
        && isKeyOrTrustStorePathNullOrHasFileScheme(tlsConfig.getTrustStorePath())) {
      enableAutoRenewalFromFileStoreForSSLFactory(sslFactory, tlsConfig);
    }
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
   * check if the key store or trust store path is null or has file scheme.
   *
   * @param keyOrTrustStorePath key store or trust store path in String format.
   */
  public static boolean isKeyOrTrustStorePathNullOrHasFileScheme(String keyOrTrustStorePath) {
    try {
      return keyOrTrustStorePath == null
          || makeKeyOrTrustStoreUrl(keyOrTrustStorePath).toURI().getScheme().startsWith(FILE_SCHEME);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Enables auto renewal of SSLFactory when
   * 1. the {@link SSLFactory} is created with a key manager and trust manager swappable
   * 2. the key store is null or a local file
   * 3. the trust store is null or a local file
   * 4. the key store or trust store file changes.
   * @param sslFactory the {@link SSLFactory} to enable key manager and trust manager auto renewal
   * @param tlsConfig the {@link TlsConfig} to get the key store and trust store information
   */
  public static void enableAutoRenewalFromFileStoreForSSLFactory(SSLFactory sslFactory, TlsConfig tlsConfig) {
    enableAutoRenewalFromFileStoreForSSLFactory(sslFactory,
        tlsConfig.getKeyStoreType(), tlsConfig.getKeyStorePath(), tlsConfig.getKeyStorePassword(),
        tlsConfig.getTrustStoreType(), tlsConfig.getTrustStorePath(), tlsConfig.getTrustStorePassword(),
        null, null);
  }

  private static void enableAutoRenewalFromFileStoreForSSLFactory(
      SSLFactory sslFactory,
      String keyStoreType, String keyStorePath, String keyStorePassword,
      String trustStoreType, String trustStorePath, String trustStorePassword,
      String sslContextProtocol, SecureRandom secureRandom) {
    try {
      URL keyStoreURL = keyStorePath == null ? null : makeKeyOrTrustStoreUrl(keyStorePath);
      URL trustStoreURL = trustStorePath == null ? null : makeKeyOrTrustStoreUrl(trustStorePath);
      if (keyStoreURL != null) {
        Preconditions.checkArgument(
            keyStoreURL.toURI().getScheme().startsWith(FILE_SCHEME),
            "key store path must be a local file path or null when SSL auto renew is enabled");
        Preconditions.checkArgument(
            sslFactory.getKeyManager().isPresent()
                && sslFactory.getKeyManager().get() instanceof HotSwappableX509ExtendedKeyManager,
            "key manager of the existing SSLFactory must be swappable"
        );
      }
      if (trustStoreURL != null) {
        Preconditions.checkArgument(
            trustStoreURL.toURI().getScheme().startsWith(FILE_SCHEME),
            "trust store path must be a local file path or null when SSL auto renew is enabled");
        Preconditions.checkArgument(
            sslFactory.getTrustManager().isPresent()
                && sslFactory.getTrustManager().get() instanceof HotSwappableX509ExtendedTrustManager,
            "trust manager of the existing SSLFactory must be swappable"
        );
      }
      // The reloadSslFactoryWhenFileStoreChanges is a blocking call, so we need to create a new thread to run it.
      // Creating a new thread to run the reloadSslFactoryWhenFileStoreChanges is costly; however, unless we
      // invoke the createAutoRenewedSSLFactoryFromFileStore method crazily, this should not be a problem.
      Executors.newSingleThreadExecutor().execute(() -> {
        try {
          reloadSslFactoryWhenFileStoreChanges(sslFactory,
              keyStoreType, keyStorePath, keyStorePassword,
              trustStoreType, trustStorePath, trustStorePassword,
              sslContextProtocol, secureRandom);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  static void reloadSslFactoryWhenFileStoreChanges(SSLFactory baseSslFactory,
      String keyStoreType, String keyStorePath, String keyStorePassword,
      String trustStoreType, String trustStorePath, String trustStorePassword,
      String sslContextProtocol, SecureRandom secureRandom)
      throws IOException, URISyntaxException, InterruptedException {
    LOGGER.info("Enable auto renewal of SSLFactory {} when key store {} or trust store {} changes",
        baseSslFactory, keyStorePath, trustStorePath);
    WatchService watchService = FileSystems.getDefault().newWatchService();
    Map<WatchKey, Set<Path>> watchKeyPathMap = new HashMap<>();
    registerFile(watchService, watchKeyPathMap, keyStorePath);
    registerFile(watchService, watchKeyPathMap, trustStorePath);
    int maxSslFactoryReloadingAttempts = 3;
    int sslFactoryReloadingRetryDelayMs = 1000;
    WatchKey key;
    while ((key = watchService.take()) != null) {
      for (WatchEvent<?> event : key.pollEvents()) {
        Path changedFile = (Path) event.context();
        if (watchKeyPathMap.get(key).contains(changedFile)) {
          LOGGER.info("Detected change in file: {}, try to renew SSLFactory {} "
              + "(built from key store {} and truststore {})",
              changedFile, baseSslFactory, keyStorePath, trustStorePath);
          try {
            // Need to retry a few times because when one file (key store or trust store) is updated, the other file
            // (trust store or key store) may not have been fully written yet, so we need to wait a bit and retry.
            RetryPolicies.fixedDelayRetryPolicy(maxSslFactoryReloadingAttempts, sslFactoryReloadingRetryDelayMs)
                .attempt(() -> {
                  try {
                    SSLFactory updatedSslFactory =
                        createSSLFactory(keyStoreType, keyStorePath, keyStorePassword, trustStoreType, trustStorePath,
                            trustStorePassword, sslContextProtocol, secureRandom, false);
                    SSLFactoryUtils.reload(baseSslFactory, updatedSslFactory);
                    LOGGER.info("Successfully renewed SSLFactory {} (built from key store {} and truststore {}) on file"
                        + " {} changes", baseSslFactory, keyStorePath, trustStorePath, changedFile);
                    return true;
                  } catch (Exception e) {
                    LOGGER.info(
                        "Encountered issues when renewing SSLFactory {} (built from key store {} and truststore {}) on "
                            + "file {} changes", baseSslFactory, keyStorePath, trustStorePath, changedFile, e);
                    return false;
                  }
                });
          } catch (Exception e) {
            LOGGER.error(
                "Failed to renew SSLFactory {} (built from key store {} and truststore {}) on file {} changes after {} "
                    + "retries", baseSslFactory, keyStorePath, trustStorePath, changedFile,
                maxSslFactoryReloadingAttempts, e);
          }
        }
      }
      key.reset();
    }
  }

  @VisibleForTesting
  static void registerFile(WatchService watchService, Map<WatchKey, Set<Path>> keyPathMap, String filePath)
      throws IOException, URISyntaxException {
    if (filePath == null) {
      return;
    }
    Path path = Path.of(makeKeyOrTrustStoreUrl(filePath).getPath());
    WatchKey key = path.getParent().register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
    keyPathMap.computeIfAbsent(key, k -> new HashSet<>());
    keyPathMap.get(key).add(path.getFileName());
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
        null, null, true);
  }

  @VisibleForTesting
  static SSLFactory createSSLFactory(
      String keyStoreType, String keyStorePath, String keyStorePassword,
      String trustStoreType, String trustStorePath, String trustStorePassword,
      String sslContextProtocol, SecureRandom secureRandom, boolean keyAndTrustMaterialSwappable) {
    try {
      SSLFactory.Builder sslFactoryBuilder = SSLFactory.builder();
      InputStream keyStoreStream = null;
      InputStream trustStoreStream = null;
      if (keyStorePath != null) {
        Preconditions.checkNotNull(keyStorePassword, "key store password must not be null");
        keyStoreStream = makeKeyOrTrustStoreUrl(keyStorePath).openStream();
        if (keyAndTrustMaterialSwappable) {
          sslFactoryBuilder.withSwappableIdentityMaterial();
        }
        sslFactoryBuilder.withIdentityMaterial(keyStoreStream, keyStorePassword.toCharArray(), keyStoreType);
      }
      if (trustStorePath != null) {
        Preconditions.checkNotNull(trustStorePassword, "trust store password must not be null");
        trustStoreStream = makeKeyOrTrustStoreUrl(trustStorePath).openStream();
        if (keyAndTrustMaterialSwappable) {
          sslFactoryBuilder.withSwappableTrustMaterial();
        }
        sslFactoryBuilder.withTrustMaterial(trustStoreStream, trustStorePassword.toCharArray(), trustStoreType);
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
      LOGGER.info("Successfully created SSLFactory {} with key store {} and trust store {}. "
              + "Key and trust material swappable: {}",
          sslFactory, keyStorePath, trustStorePath, keyAndTrustMaterialSwappable);
      return sslFactory;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
