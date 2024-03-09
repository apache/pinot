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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.keymanager.HotSwappableX509ExtendedKeyManager;
import nl.altindag.ssl.trustmanager.HotSwappableX509ExtendedTrustManager;
import nl.altindag.ssl.util.SSLFactoryUtils;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for shared renewable TLS configuration logic
 */
public class RenewableTlsUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(RenewableTlsUtils.class);
  private static final String FILE_SCHEME = "file";

  private RenewableTlsUtils() {
    // left blank
  }


  /**
   * Create a {@link SSLFactory} instance with identity material and trust material swappable for a given TlsConfig,
   * and nables auto renewal of the {@link SSLFactory} instance when
   * 1. the {@link SSLFactory} is created with a key manager and trust manager swappable
   * 2. the key store is null or a local file
   * 3. the trust store is null or a local file
   * 4. the key store or trust store file changes.
   * @param tlsConfig {@link TlsConfig}
   * @return a {@link SSLFactory} instance with identity material and trust material swappable
   */
  public static SSLFactory createSSLFactoryAndEnableAutoRenewalWhenUsingFileStores(TlsConfig tlsConfig) {
    SSLFactory sslFactory = createSSLFactory(tlsConfig);
    if (TlsUtils.isKeyOrTrustStorePathNullOrHasFileScheme(tlsConfig.getKeyStorePath())
        && TlsUtils.isKeyOrTrustStorePathNullOrHasFileScheme(tlsConfig.getTrustStorePath())) {
      enableAutoRenewalFromFileStoreForSSLFactory(sslFactory, tlsConfig);
    }
    return sslFactory;
  }

  /**
   * Create a {@link SSLFactory} instance with identity material and trust material swappable for a given TlsConfig
   * @param tlsConfig {@link TlsConfig}
   * @return a {@link SSLFactory} instance with identity material and trust material swappable
   */
  private static SSLFactory createSSLFactory(TlsConfig tlsConfig) {
    return createSSLFactory(
        tlsConfig.getKeyStoreType(), tlsConfig.getKeyStorePath(), tlsConfig.getKeyStorePassword(),
        tlsConfig.getTrustStoreType(), tlsConfig.getTrustStorePath(), tlsConfig.getTrustStorePassword(),
        null, null, true, tlsConfig.isInsecure());
  }

  static SSLFactory createSSLFactory(
      String keyStoreType, String keyStorePath, String keyStorePassword,
      String trustStoreType, String trustStorePath, String trustStorePassword,
      String sslContextProtocol, SecureRandom secureRandom, boolean keyAndTrustMaterialSwappable, boolean isInsecure) {
    try {
      SSLFactory.Builder sslFactoryBuilder = SSLFactory.builder();
      InputStream keyStoreStream = null;
      InputStream trustStoreStream = null;
      if (keyStorePath != null) {
        Preconditions.checkNotNull(keyStorePassword, "key store password must not be null");
        keyStoreStream = TlsUtils.makeKeyOrTrustStoreUrl(keyStorePath).openStream();
        if (keyAndTrustMaterialSwappable) {
          sslFactoryBuilder.withSwappableIdentityMaterial();
        }
        sslFactoryBuilder.withIdentityMaterial(keyStoreStream, keyStorePassword.toCharArray(), keyStoreType);
      }
      if (isInsecure) {
        if (keyAndTrustMaterialSwappable) {
          sslFactoryBuilder.withSwappableTrustMaterial();
        }
        sslFactoryBuilder.withUnsafeTrustMaterial();
      } else if (trustStorePath != null) {
        Preconditions.checkNotNull(trustStorePassword, "trust store password must not be null");
        trustStoreStream = TlsUtils.makeKeyOrTrustStoreUrl(trustStorePath).openStream();
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

  /**
   * Enables auto renewal of SSLFactory when
   * 1. the {@link SSLFactory} is created with a key manager and trust manager swappable
   * 2. the key store is null or a local file
   * 3. the trust store is null or a local file
   * 4. the key store or trust store file changes.
   * @param sslFactory the {@link SSLFactory} to enable key manager and trust manager auto renewal
   * @param tlsConfig the {@link TlsConfig} to get the key store and trust store information
   */
  @VisibleForTesting
  static void enableAutoRenewalFromFileStoreForSSLFactory(SSLFactory sslFactory, TlsConfig tlsConfig) {
    enableAutoRenewalFromFileStoreForSSLFactory(sslFactory,
        tlsConfig.getKeyStoreType(), tlsConfig.getKeyStorePath(), tlsConfig.getKeyStorePassword(),
        tlsConfig.getTrustStoreType(), tlsConfig.getTrustStorePath(), tlsConfig.getTrustStorePassword(),
        null, null, tlsConfig.isInsecure());
  }

  static void enableAutoRenewalFromFileStoreForSSLFactory(SSLFactory sslFactory, String keyStoreType,
      String keyStorePath, String keyStorePassword, String trustStoreType, String trustStorePath,
      String trustStorePassword, String sslContextProtocol, SecureRandom secureRandom, boolean isInsecure) {
    try {
      URL keyStoreURL = keyStorePath == null ? null : TlsUtils.makeKeyOrTrustStoreUrl(keyStorePath);
      URL trustStoreURL = trustStorePath == null ? null : TlsUtils.makeKeyOrTrustStoreUrl(trustStorePath);
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
              sslContextProtocol, secureRandom, isInsecure);
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
      String sslContextProtocol, SecureRandom secureRandom, boolean isInsecure)
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
                            trustStorePassword, sslContextProtocol, secureRandom, false, isInsecure);
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
    Path path = Path.of(TlsUtils.makeKeyOrTrustStoreUrl(filePath).getPath());
    WatchKey key = path.getParent().register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
    keyPathMap.computeIfAbsent(key, k -> new HashSet<>());
    keyPathMap.get(key).add(path.getFileName());
  }
}
