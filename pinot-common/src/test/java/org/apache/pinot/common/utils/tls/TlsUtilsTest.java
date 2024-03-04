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

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import nl.altindag.ssl.SSLFactory;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.config.TlsConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Command used to generated the keystore and truststore files:
 * 1. keytool -genkeypair -keyalg RSA -keysize 2048 -keystore keystore.p12 -storetype PKCS12 -storepass changeit
 * -keypass changeit -validity 18250
 * 2. keytool -export -alias mykey -file certificate.cer -keystore keystore.p12 -storetype PKCS12 -storepass changeit
 * 3. keytool -import -alias mykey -file certificate.cer -keystore truststore.p12 -storetype PKCS12 -storepass changeit
 * -noprompt
 */
public class TlsUtilsTest {
  private static final String TLS_RESOURCE_FOLDER = "tls/";
  private static final String TLS_KEYSTORE_FILE = "keystore.p12";
  private static final String TLS_TRUSTSTORE_FILE = "truststore.p12";
  private static final String TLS_KEYSTORE_UPDATED_FILE = "keystore-updated.p12";
  private static final String TLS_TRUSTSTORE_UPDATED_FILE = "truststore-updated.p12";
  private static final String PASSWORD = "changeit";
  private static final String KEYSTORE_TYPE = "PKCS12";
  private static final String TRUSTSTORE_TYPE = "PKCS12";
  private static final String DEFAULT_TEST_TLS_DIR
      = new File(FileUtils.getTempDirectoryPath(), "test-tls-dir" + System.currentTimeMillis()).getAbsolutePath();
  private static final String KEY_NAME_ALIAS = "mykey";

  private static final String TLS_KEYSTORE_FILE_PATH = DEFAULT_TEST_TLS_DIR + "/" + TLS_KEYSTORE_FILE;
  private static final String TLS_TRUSTSTORE_FILE_PATH = DEFAULT_TEST_TLS_DIR + "/" + TLS_TRUSTSTORE_FILE;

  @BeforeClass
  public void setUp()
      throws IOException, URISyntaxException {
    copyResourceFilesToTempFolder(
        ImmutableMap.of(TLS_KEYSTORE_FILE, TLS_KEYSTORE_FILE, TLS_TRUSTSTORE_FILE, TLS_TRUSTSTORE_FILE));
  }

  private static void copyResourceFilesToTempFolder(Map<String, String> srcAndDestFileMap)
      throws URISyntaxException, IOException {
    // Create the destination folder if it doesn't exist
    Files.createDirectories(Paths.get(DEFAULT_TEST_TLS_DIR));
    for (Map.Entry<String, String> entry : srcAndDestFileMap.entrySet()) {
      // Use the class loader to get the InputStream of the resource file
      try (InputStream resourceStream
          = TlsUtilsTest.class.getClassLoader().getResourceAsStream(TLS_RESOURCE_FOLDER + entry.getKey())) {
        if (resourceStream == null) {
          throw new IOException("Resource file not found: " + entry.getKey());
        }
        // Specify the destination path
        Path destinationPath = Paths.get(DEFAULT_TEST_TLS_DIR, entry.getValue());
        // Use Files.copy to copy the file to the destination folder
        Files.copy(resourceStream, destinationPath, StandardCopyOption.REPLACE_EXISTING);
      } catch (IOException e) {
        e.printStackTrace(); // Handle the exception as needed
      }
    }
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(new File(DEFAULT_TEST_TLS_DIR));
  }

  @Test
  public void swappableSSLFactoryHasSameAsStaticOnes()
      throws NoSuchAlgorithmException, KeyManagementException, IOException, URISyntaxException {
    SecureRandom secureRandom = new SecureRandom();
    KeyManagerFactory keyManagerFactory =
        TlsUtils.createKeyManagerFactory(TLS_KEYSTORE_FILE_PATH, PASSWORD, KEYSTORE_TYPE);
    TrustManagerFactory trustManagerFactory =
        TlsUtils.createTrustManagerFactory(TLS_TRUSTSTORE_FILE_PATH, PASSWORD, TRUSTSTORE_TYPE);
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), secureRandom);
    SSLFactory sslFactory =
        RenewableTlsUtils.createSSLFactory(KEYSTORE_TYPE, TLS_KEYSTORE_FILE_PATH, PASSWORD,
            TRUSTSTORE_TYPE, TLS_TRUSTSTORE_FILE_PATH, PASSWORD, "TLS", secureRandom, true, false);
    KeyManagerFactory swappableKeyManagerFactory = sslFactory.getKeyManagerFactory().get();
    assertEquals(swappableKeyManagerFactory.getKeyManagers().length, keyManagerFactory.getKeyManagers().length);
    assertEquals(swappableKeyManagerFactory.getKeyManagers().length, 1);
    assertSSLKeyManagersEqual(swappableKeyManagerFactory.getKeyManagers()[0], keyManagerFactory.getKeyManagers()[0]);
    TrustManagerFactory swappableTrustManagerFactory = sslFactory.getTrustManagerFactory().get();
    assertEquals(swappableTrustManagerFactory.getTrustManagers().length,
        trustManagerFactory.getTrustManagers().length);
    assertEquals(swappableTrustManagerFactory.getTrustManagers().length, 1);
    assertSSLTrustManagersEqual(swappableTrustManagerFactory.getTrustManagers()[0],
        trustManagerFactory.getTrustManagers()[0]);
    SSLContext swappableSSLContext = sslFactory.getSslContext();
    assertEquals(swappableSSLContext.getProtocol(), sslContext.getProtocol());
    assertEquals(swappableSSLContext.getProvider(), sslContext.getProvider());
  }

  private static void assertSSLKeyManagersEqual(KeyManager km1, KeyManager km2) {
    X509KeyManager x509KeyManager1 = (X509KeyManager) km1;
    X509KeyManager x509KeyManager2 = (X509KeyManager) km2;
    assertEquals(x509KeyManager1.getPrivateKey(KEY_NAME_ALIAS), x509KeyManager2.getPrivateKey(KEY_NAME_ALIAS));

    Certificate[] certs1 = x509KeyManager1.getCertificateChain(KEY_NAME_ALIAS);
    Certificate[] certs2 = x509KeyManager2.getCertificateChain(KEY_NAME_ALIAS);
    assertEquals(certs1.length, certs2.length);
    assertEquals(certs1.length, 1);
    assertEquals(certs1[0], certs2[0]);
  }

  private static void assertSSLTrustManagersEqual(TrustManager tm1, TrustManager tm2) {
    X509TrustManager x509TrustManager1 = (X509TrustManager) tm1;
    X509TrustManager x509TrustManager2 = (X509TrustManager) tm2;

    assertEquals(x509TrustManager1.getAcceptedIssuers().length, x509TrustManager2.getAcceptedIssuers().length);
    assertEquals(x509TrustManager1.getAcceptedIssuers().length, 1);
    assertEquals(x509TrustManager1.getAcceptedIssuers()[0], x509TrustManager2.getAcceptedIssuers()[0]);
  }

  @Test
  public void reloadSslFactoryWhenFileStoreChanges()
      throws IOException, URISyntaxException, InterruptedException {
    SecureRandom secureRandom = new SecureRandom();
    SSLFactory sslFactory = RenewableTlsUtils.createSSLFactory(KEYSTORE_TYPE, TLS_KEYSTORE_FILE_PATH, PASSWORD,
        TRUSTSTORE_TYPE, TLS_TRUSTSTORE_FILE_PATH, PASSWORD, "TLS", secureRandom, true, false);
    X509ExtendedKeyManager x509ExtendedKeyManager = sslFactory.getKeyManager().get();
    X509ExtendedTrustManager x509ExtendedTrustManager = sslFactory.getTrustManager().get();
    SSLContext sslContext = sslFactory.getSslContext();

    PrivateKey privateKey = x509ExtendedKeyManager.getPrivateKey(KEY_NAME_ALIAS);
    Certificate certForPrivateKey = x509ExtendedKeyManager.getCertificateChain(KEY_NAME_ALIAS)[0];
    X509Certificate acceptedIssuerForCert = x509ExtendedTrustManager.getAcceptedIssuers()[0];

    // start a new thread to reload the ssl factory when the tls files change
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.execute(
        () -> {
          try {
            RenewableTlsUtils.reloadSslFactoryWhenFileStoreChanges(sslFactory, KEYSTORE_TYPE, TLS_KEYSTORE_FILE_PATH,
                PASSWORD, TRUSTSTORE_TYPE, TLS_TRUSTSTORE_FILE_PATH, PASSWORD, "TLS", secureRandom, false);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    WatchService watchService = FileSystems.getDefault().newWatchService();
    Map<WatchKey, Set<Path>> watchKeyPathMap = new HashMap<>();
    RenewableTlsUtils.registerFile(watchService, watchKeyPathMap, TLS_KEYSTORE_FILE_PATH);
    RenewableTlsUtils.registerFile(watchService, watchKeyPathMap, TLS_TRUSTSTORE_FILE_PATH);

    // wait for the new thread to start
    Thread.sleep(100);

    // update tls files
    copyResourceFilesToTempFolder(
        ImmutableMap.of(TLS_KEYSTORE_UPDATED_FILE, TLS_KEYSTORE_FILE, TLS_TRUSTSTORE_UPDATED_FILE,
            TLS_TRUSTSTORE_FILE));

    // wait for the file change event to be detected
    watchService.take();
    // it will take some time for the thread to be notified and reload the ssl factory
    Thread.sleep(500);
    executorService.shutdown();

    // after tls file update, the returned values should be the same, since the wrapper is the same
    X509ExtendedKeyManager udpatedX509ExtendedKeyManager = sslFactory.getKeyManager().get();
    X509ExtendedTrustManager updatedX509ExtendedTrustManager = sslFactory.getTrustManager().get();
    SSLContext updatedSslContext = sslFactory.getSslContext();
    assertEquals(x509ExtendedKeyManager, udpatedX509ExtendedKeyManager);
    assertEquals(x509ExtendedTrustManager, updatedX509ExtendedTrustManager);
    assertEquals(sslContext, updatedSslContext);

    // after tls file update, the underlying values should be different
    assertNotEquals(privateKey, udpatedX509ExtendedKeyManager.getPrivateKey(KEY_NAME_ALIAS));
    assertNotEquals(certForPrivateKey, udpatedX509ExtendedKeyManager.getCertificateChain(KEY_NAME_ALIAS)[0]);
    assertNotEquals(acceptedIssuerForCert, updatedX509ExtendedTrustManager.getAcceptedIssuers()[0]);
  }

  @Test
  public void enableAutoRenewalFromFileStoreForSSLFactoryThrows() {
    SSLFactory swappableSslFactory =
        RenewableTlsUtils.createSSLFactory(KEYSTORE_TYPE, TLS_KEYSTORE_FILE_PATH, PASSWORD, TRUSTSTORE_TYPE,
            TLS_TRUSTSTORE_FILE_PATH, PASSWORD, "TLS", null, true, false);
    TlsConfig tlsConfig = new TlsConfig();
    tlsConfig.setKeyStoreType(KEYSTORE_TYPE);
    tlsConfig.setKeyStorePath("ftp://" + TLS_KEYSTORE_FILE_PATH);
    tlsConfig.setKeyStorePassword(PASSWORD);
    tlsConfig.setTrustStoreType(TRUSTSTORE_TYPE);
    tlsConfig.setTrustStorePath(TLS_TRUSTSTORE_FILE_PATH);
    tlsConfig.setTrustStorePassword(PASSWORD);
    RuntimeException e = null;
    try {
      RenewableTlsUtils.enableAutoRenewalFromFileStoreForSSLFactory(swappableSslFactory, tlsConfig);
    } catch (RuntimeException ex) {
      e = ex;
    }
    assertNotNull(e);
    assertEquals(e.getMessage(), "java.lang.IllegalArgumentException: key store path must be a local file path "
        + "or null when SSL auto renew is enabled");

    tlsConfig.setKeyStorePath(TLS_KEYSTORE_FILE_PATH);
    tlsConfig.setTrustStorePath("ftp://" + TLS_TRUSTSTORE_FILE_PATH);
    e = null;
    try {
      RenewableTlsUtils.enableAutoRenewalFromFileStoreForSSLFactory(swappableSslFactory, tlsConfig);
    } catch (RuntimeException ex) {
      e = ex;
    }
    assertEquals(e.getMessage(), "java.lang.IllegalArgumentException: trust store path must be a local file path "
        + "or null when SSL auto renew is enabled");

    SSLFactory nonSwappableSslFactory =
        RenewableTlsUtils.createSSLFactory(KEYSTORE_TYPE, TLS_KEYSTORE_FILE_PATH, PASSWORD, TRUSTSTORE_TYPE,
            TLS_TRUSTSTORE_FILE_PATH, PASSWORD, "TLS", null, false, false);
    e = null;
    tlsConfig.setTrustStorePath(TLS_TRUSTSTORE_FILE_PATH);
    try {
      RenewableTlsUtils.enableAutoRenewalFromFileStoreForSSLFactory(nonSwappableSslFactory, tlsConfig);
    } catch (RuntimeException ex) {
      e = ex;
    }
    assertEquals(e.getMessage(),
        "java.lang.IllegalArgumentException: key manager of the existing SSLFactory must be swappable");

    tlsConfig.setKeyStorePath(null);
    e = null;
    try {
      RenewableTlsUtils.enableAutoRenewalFromFileStoreForSSLFactory(nonSwappableSslFactory, tlsConfig);
    } catch (RuntimeException ex) {
      e = ex;
    }
    assertEquals(e.getMessage(),
        "java.lang.IllegalArgumentException: trust manager of the existing SSLFactory must be swappable");
  }
}
