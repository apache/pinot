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
package org.apache.pinot.plugin.stream.kafka20;

import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SSL utils class which helps in initialization of Kafka client SSL configuration. The class can install the
 * provided server certificate enabling one-way SSL or it can install the server certificate and the
 * client certificates enabling two-way SSL.
 */
public class KafkaSSLUtils {

  private KafkaSSLUtils() {
    // private on purpose
  }
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSSLUtils.class);
  // Value constants
  private static final String DEFAULT_CERTIFICATE_TYPE = "X.509";
  private static final String DEFAULT_KEY_ALGORITHM = "RSA";
  private static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";
  private static final String DEFAULT_SECURITY_PROTOCOL = "SSL";
  private static final String DEFAULT_TRUSTSTORE_TYPE = "jks";
  private static final String DEFAULT_SERVER_ALIAS = "ServerAlias";
  private static final String DEFAULT_CLIENT_ALIAS = "ClientAlias";
  // Key constants
  private static final String SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location";
  private static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";
  private static final String SECURITY_PROTOCOL = "security.protocol";
  private static final String SSL_KEYSTORE_LOCATION = "ssl.keystore.location";
  private static final String SSL_KEYSTORE_PASSWORD = "ssl.keystore.password";
  private static final String SSL_KEY_PASSWORD = "ssl.key.password";
  private static final String STREAM_KAFKA_SSL_SERVER_CERTIFICATE = "stream.kafka.ssl.server.certificate";
  private static final String STREAM_KAFKA_SSL_CERTIFICATE_TYPE = "stream.kafka.ssl.certificate.type";
  private static final String SSL_TRUSTSTORE_TYPE = "ssl.truststore.type";
  private static final String STREAM_KAFKA_SSL_CLIENT_CERTIFICATE = "stream.kafka.ssl.client.certificate";
  private static final String STREAM_KAFKA_SSL_CLIENT_KEY = "stream.kafka.ssl.client.key";
  private static final String STREAM_KAFKA_SSL_CLIENT_KEY_ALGORITHM = "stream.kafka.ssl.client.key.algorithm";
  private static final String SSL_KEYSTORE_TYPE = "ssl.keystore.type";

  public static void initSSL(Properties consumerProps) {
    // Check if one-way SSL is enabled. In this scenario, the client validates the server certificate.
    String trustStoreLocation = consumerProps.getProperty(SSL_TRUSTSTORE_LOCATION);
    String trustStorePassword = consumerProps.getProperty(SSL_TRUSTSTORE_PASSWORD);
    String serverCertificate = consumerProps.getProperty(STREAM_KAFKA_SSL_SERVER_CERTIFICATE);
    if (StringUtils.isAnyEmpty(trustStoreLocation, trustStorePassword, serverCertificate)) {
      LOGGER.info("Skipping auto SSL server validation since it's not configured.");
      return;
    }
    if (shouldRenewTrustStore(consumerProps)) {
      initTrustStore(consumerProps);
    }

    // Set the security protocol
    String securityProtocol = consumerProps.getProperty(SECURITY_PROTOCOL, DEFAULT_SECURITY_PROTOCOL);
    consumerProps.setProperty(SECURITY_PROTOCOL, securityProtocol);

    // Check if two-way SSL is enabled. In this scenario, the client validates the server's certificate and the server
    // validates the client's certificate.
    String keyStoreLocation = consumerProps.getProperty(SSL_KEYSTORE_LOCATION);
    String keyStorePassword = consumerProps.getProperty(SSL_KEYSTORE_PASSWORD);
    String keyPassword = consumerProps.getProperty(SSL_KEY_PASSWORD);
    String clientCertificate = consumerProps.getProperty(STREAM_KAFKA_SSL_CLIENT_CERTIFICATE);
    if (StringUtils.isAnyEmpty(keyStoreLocation, keyStorePassword, keyPassword, clientCertificate)) {
      LOGGER.info("Skipping auto SSL client validation since it's not configured.");
      return;
    }
    if (shouldRenewKeyStore(consumerProps)) {
      initKeyStore(consumerProps);
    }
  }

  @VisibleForTesting
  static void initTrustStore(Properties consumerProps) {
    Path trustStorePath = getTrustStorePath(consumerProps);
    if (Files.exists(trustStorePath)) {
      deleteFile(trustStorePath);
    }
    LOGGER.info("Initializing the SSL trust store");
    try {
      // Create the trust store path
      createFile(trustStorePath);
    } catch (FileAlreadyExistsException fex) {
      LOGGER.warn("SSL trust store initialization failed as trust store already exists.");
      return;
    } catch (IOException iex) {
      throw new RuntimeException(String.format("Failed to create the trust store path: %s", trustStorePath), iex);
    }

    try {
      String trustStorePassword = consumerProps.getProperty(SSL_TRUSTSTORE_PASSWORD);
      String serverCertificate = consumerProps.getProperty(STREAM_KAFKA_SSL_SERVER_CERTIFICATE);
      String certificateType = consumerProps.getProperty(STREAM_KAFKA_SSL_CERTIFICATE_TYPE, DEFAULT_CERTIFICATE_TYPE);
      String trustStoreType = consumerProps.getProperty(SSL_TRUSTSTORE_TYPE, DEFAULT_TRUSTSTORE_TYPE);
      consumerProps.setProperty(SSL_TRUSTSTORE_TYPE, trustStoreType);

      // Decode the Base64 string
      byte[] certBytes = Base64.getDecoder().decode(serverCertificate);
      InputStream certInputStream = new ByteArrayInputStream(certBytes);

      // Create a Certificate object
      CertificateFactory certificateFactory = CertificateFactory.getInstance(certificateType);
      Certificate certificate = certificateFactory.generateCertificate(certInputStream);

      // Create a TrustStore and load the default TrustStore
      KeyStore trustStore = KeyStore.getInstance(trustStoreType);

      // Initialize the TrustStore
      trustStore.load(null, null);

      // Add the server certificate to the truststore
      trustStore.setCertificateEntry(DEFAULT_SERVER_ALIAS, certificate);

      // Save the keystore to a file
      try (FileOutputStream fos = new FileOutputStream(trustStorePath.toString())) {
        trustStore.store(fos, trustStorePassword.toCharArray());
      }
      LOGGER.info("Initialized the SSL trust store.");
    } catch (Exception ex) {
      throw new RuntimeException("Error initializing the SSL trust store", ex);
    }
  }

  @VisibleForTesting
  static void initKeyStore(Properties consumerProps) {
    Path keyStorePath = getKeyStorePath(consumerProps);
    if (Files.exists(keyStorePath)) {
      deleteFile(keyStorePath);
    }
    LOGGER.info("Initializing the SSL key store");
    try {
      // Create the key store path
      createFile(keyStorePath);
    } catch (FileAlreadyExistsException fex) {
      LOGGER.warn("SSL key store initialization failed as key store already exists.");
      return;
    } catch (IOException iex) {
      throw new RuntimeException(String.format("Failed to create the key store path: %s", keyStorePath), iex);
    }

    String keyStorePassword = consumerProps.getProperty(SSL_KEYSTORE_PASSWORD);
    String keyPassword = consumerProps.getProperty(SSL_KEY_PASSWORD);
    String clientCertificate = consumerProps.getProperty(STREAM_KAFKA_SSL_CLIENT_CERTIFICATE);
    String certificateType = consumerProps.getProperty(STREAM_KAFKA_SSL_CERTIFICATE_TYPE, DEFAULT_CERTIFICATE_TYPE);
    String privateKeyString = consumerProps.getProperty(STREAM_KAFKA_SSL_CLIENT_KEY);
    String privateKeyAlgorithm = consumerProps.getProperty(STREAM_KAFKA_SSL_CLIENT_KEY_ALGORITHM,
        DEFAULT_KEY_ALGORITHM);
    String keyStoreType = consumerProps.getProperty(SSL_KEYSTORE_TYPE, DEFAULT_KEYSTORE_TYPE);
    consumerProps.setProperty(SSL_KEYSTORE_TYPE, keyStoreType);

    try {
      // decode the private key and certificate into bytes
      byte[] pkBytes = Base64.getDecoder().decode(privateKeyString);
      byte[] certBytes = Base64.getDecoder().decode(clientCertificate);

      // Create the private key object
      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(pkBytes);
      KeyFactory keyFactory = KeyFactory.getInstance(privateKeyAlgorithm);
      PrivateKey privateKey = keyFactory.generatePrivate(keySpec);

      // Create the Certificate object
      CertificateFactory certFactory = CertificateFactory.getInstance(certificateType);
      InputStream certInputStream = new ByteArrayInputStream(certBytes);
      Certificate certificate = certFactory.generateCertificate(certInputStream);

      // Create a KeyStore object and load a new empty keystore
      KeyStore keyStore = KeyStore.getInstance(keyStoreType);
      keyStore.load(null, null);

      // Add the key pair and certificate to the keystore
      KeyStore.PrivateKeyEntry privateKeyEntry = new KeyStore.PrivateKeyEntry(
          privateKey, new Certificate[]{certificate}
      );
      KeyStore.PasswordProtection keyPasswordProtection = new KeyStore.PasswordProtection(keyPassword.toCharArray());
      keyStore.setEntry(DEFAULT_CLIENT_ALIAS, privateKeyEntry, keyPasswordProtection);

      // Save the keystore to the specified location
      try (FileOutputStream fos = new FileOutputStream(keyStorePath.toString())) {
        keyStore.store(fos, keyStorePassword.toCharArray());
      }
      LOGGER.info("Initialized the SSL key store.");
    } catch (Exception ex) {
      throw new RuntimeException("Error initializing the SSL key store", ex);
    }
  }

  private static Path getTrustStorePath(Properties consumerProps) {
    String trustStoreLocation = consumerProps.getProperty(SSL_TRUSTSTORE_LOCATION);
    return Paths.get(trustStoreLocation);
  }

  private static Path getKeyStorePath(Properties consumerProps) {
    String keyStoreLocation = consumerProps.getProperty(SSL_KEYSTORE_LOCATION);
    return Paths.get(keyStoreLocation);
  }

  // Renew the trust store if needed.
  private static boolean shouldRenewTrustStore(Properties consumerProps) {
    boolean renewTrustStore;
    Path trustStorePath = getTrustStorePath(consumerProps);
    String trustStorePassword = consumerProps.getProperty(SSL_TRUSTSTORE_PASSWORD);
    String serverCertificate = consumerProps.getProperty(STREAM_KAFKA_SSL_SERVER_CERTIFICATE);
    String certificateType = consumerProps.getProperty(STREAM_KAFKA_SSL_CERTIFICATE_TYPE, DEFAULT_CERTIFICATE_TYPE);

    try {
      // Load the trust store
      KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      try (FileInputStream fis = new FileInputStream(trustStorePath.toString())) {
        trustStore.load(fis, trustStorePassword.toCharArray());
      }

      // Decode the provided certificate
      byte[] decodedCertBytes = Base64.getDecoder().decode(serverCertificate);
      CertificateFactory certFactory = CertificateFactory.getInstance(certificateType);
      Certificate providedCertificate = certFactory.generateCertificate(new ByteArrayInputStream(decodedCertBytes));

      // Get the certificate from the trust store
      Certificate trustStoreCertificate = trustStore.getCertificate(DEFAULT_SERVER_ALIAS);

      // Compare the certificates
      renewTrustStore = !providedCertificate.equals(trustStoreCertificate);
    } catch (FileNotFoundException fex) {
      // create the trust store if trust store does not exist – happens the very first time
      renewTrustStore = true;
    } catch (Exception ex) {
      // renew trust store if comparison check fails
      renewTrustStore = true;
      LOGGER.warn("Trust store certificate comparison check failed.", ex);
    }

    return renewTrustStore;
  }

  // Renew the key store if needed.
  private static boolean shouldRenewKeyStore(Properties consumerProps) {
    boolean renewKeyStore;
    Path keyStorePath = getKeyStorePath(consumerProps);
    String keyStorePassword = consumerProps.getProperty(SSL_KEYSTORE_PASSWORD);
    String keyPassword = consumerProps.getProperty(SSL_KEY_PASSWORD);
    String certificateType = consumerProps.getProperty(STREAM_KAFKA_SSL_CERTIFICATE_TYPE, DEFAULT_CERTIFICATE_TYPE);
    String clientCertificate = consumerProps.getProperty(STREAM_KAFKA_SSL_CLIENT_CERTIFICATE);
    String privateKeyAlgorithm = consumerProps.getProperty(STREAM_KAFKA_SSL_CLIENT_KEY_ALGORITHM,
        DEFAULT_KEY_ALGORITHM);
    String privateKeyString = consumerProps.getProperty(STREAM_KAFKA_SSL_CLIENT_KEY);
    try {
      // Load the KeyStore
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      try (FileInputStream fis = new FileInputStream(keyStorePath.toString())) {
        keyStore.load(fis, keyStorePassword.toCharArray());
      }

      // Extract certificate and private key from KeyStore
      Certificate keyStoreCert = keyStore.getCertificate(DEFAULT_CLIENT_ALIAS);
      PrivateKey keyStorePrivateKey = (PrivateKey) keyStore.getKey(DEFAULT_CLIENT_ALIAS, keyPassword.toCharArray());

      // Decode provided Base64 encoded certificate and private key
      CertificateFactory certFactory = CertificateFactory.getInstance(certificateType);
      Certificate providedCert = certFactory.generateCertificate(new ByteArrayInputStream(
          Base64.getDecoder().decode(clientCertificate)));
      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(privateKeyString));
      KeyFactory keyFactory = KeyFactory.getInstance(privateKeyAlgorithm);
      PrivateKey providedPrivateKey = keyFactory.generatePrivate(keySpec);

      // Compare certificates and private keys
      boolean isCertSame = Arrays.equals(keyStoreCert.getEncoded(), providedCert.getEncoded());
      boolean isKeySame = Arrays.equals(keyStorePrivateKey.getEncoded(), providedPrivateKey.getEncoded());
      renewKeyStore = !(isCertSame && isKeySame);
    } catch (FileNotFoundException fex) {
      // create the key store if key store does not exist – happens the very first time
      renewKeyStore = true;
    } catch (Exception ex) {
      // renew key store if comparison check fails
      renewKeyStore = true;
      LOGGER.warn("Key store certificate and private key comparison checks failed.", ex);
    }
    return renewKeyStore;
  }

  private static void deleteFile(Path path) {
    try {
      Files.deleteIfExists(path);
      LOGGER.info(String.format("Successfully deleted file: %s", path));
    } catch (IOException iex) {
      LOGGER.warn(String.format("Failed to delete the file: %s", path));
    }
  }

  private static void createFile(Path path)
      throws IOException {
    Path parentDir = path.getParent();
    if (parentDir != null) {
      Files.createDirectories(parentDir);
    }
    Path filePath = path.toAbsolutePath();
    if (!Files.exists(filePath)) {
      Files.createFile(filePath);
      LOGGER.info(String.format("Successfully created file: %s", path));
    }
  }
}
