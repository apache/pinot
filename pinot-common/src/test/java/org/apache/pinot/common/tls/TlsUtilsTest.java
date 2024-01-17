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
package org.apache.pinot.common.tls;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import nl.altindag.ssl.SSLFactory;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class TlsUtilsTest {
  private static final String TLS_RESOURCE_FOLDER = "tls/";
  private static final String TLS_KEYSTORE_FILE = "keystore.p12";
  private static final String TLS_TRUSTSTORE_FILE = "truststore.p12";
  private static final String[] TLS_RESOURCE_FILES = {TLS_KEYSTORE_FILE, TLS_TRUSTSTORE_FILE};
  private static final String PASSWORD = "changeit";
  private static final String KEYSTORE_TYPE = "PKCS12";
  private static final String TRUSTSTORE_TYPE = "PKCS12";
  private static final String DEFAULT_TEST_TLS_DIR
      = new File(FileUtils.getTempDirectoryPath(), "test-tls-dir" + System.currentTimeMillis()).getAbsolutePath();

  @BeforeClass
  public void setUp()
      throws IOException, URISyntaxException {
    copyResourceFilesToTempFolder();
  }

  private static void copyResourceFilesToTempFolder()
      throws URISyntaxException, IOException {
    // Create the destination folder if it doesn't exist
    Files.createDirectories(Paths.get(DEFAULT_TEST_TLS_DIR));
    for (String fileName : TLS_RESOURCE_FILES) {
      // Use the class loader to get the InputStream of the resource file
      try (InputStream resourceStream
          = TlsUtilsTest.class.getClassLoader().getResourceAsStream(TLS_RESOURCE_FOLDER + fileName)) {
        if (resourceStream == null) {
          throw new IOException("Resource file not found: " + fileName);
        }
        // Specify the destination path
        Path destinationPath = Paths.get(DEFAULT_TEST_TLS_DIR, fileName);
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
      throws NoSuchAlgorithmException, KeyManagementException {
    SecureRandom secureRandom = new SecureRandom();
    KeyManagerFactory keyManagerFactory =
        TlsUtils.createKeyManagerFactory(DEFAULT_TEST_TLS_DIR + "/" + TLS_KEYSTORE_FILE, PASSWORD,
            KEYSTORE_TYPE);
    TrustManagerFactory trustManagerFactory =
        TlsUtils.createTrustManagerFactory(DEFAULT_TEST_TLS_DIR + "/" + TLS_TRUSTSTORE_FILE, PASSWORD,
            TRUSTSTORE_TYPE);
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), secureRandom);
    SSLFactory sslFactory =
        TlsUtils.createSSLFactory(KEYSTORE_TYPE, DEFAULT_TEST_TLS_DIR + "/" + TLS_KEYSTORE_FILE, PASSWORD,
            TRUSTSTORE_TYPE, DEFAULT_TEST_TLS_DIR + "/" + TLS_TRUSTSTORE_FILE, PASSWORD,
            "TLS", secureRandom);
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
    assertEquals(x509KeyManager1.getPrivateKey("mykey"), x509KeyManager2.getPrivateKey("mykey"));

    Certificate[] certs1 = x509KeyManager1.getCertificateChain("mykey");
    Certificate[] certs2 = x509KeyManager2.getCertificateChain("mykey");
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
}
