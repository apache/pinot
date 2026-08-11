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
package org.apache.pinot.plugin.stream.kafka;

import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.interfaces.DSAKey;
import java.security.interfaces.EdECKey;
import java.security.interfaces.RSAKey;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.MGF1ParameterSpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.PSSParameterSpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// SSL utils class which helps in initialization of Kafka client SSL configuration. The class can install the
/// provided server certificate enabling one-way SSL or it can install the server certificate and the
/// client certificates enabling two-way SSL.
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
  // Follow the JVM default keystore type (typically "jks") unless explicitly configured.
  private static final String DEFAULT_TRUSTSTORE_TYPE = KeyStore.getDefaultType();
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
    if (StringUtils.isNotEmpty(serverCertificate)) {
      validateAutoSslProperties(consumerProps, SSL_TRUSTSTORE_LOCATION, SSL_TRUSTSTORE_PASSWORD,
          STREAM_KAFKA_SSL_SERVER_CERTIFICATE, STREAM_KAFKA_SSL_CERTIFICATE_TYPE, SSL_TRUSTSTORE_TYPE);

      String clientCertificate = consumerProps.getProperty(STREAM_KAFKA_SSL_CLIENT_CERTIFICATE);
      if (StringUtils.isNotEmpty(clientCertificate)) {
        validateAutoSslProperties(consumerProps, SSL_KEYSTORE_LOCATION, SSL_KEYSTORE_PASSWORD, SSL_KEY_PASSWORD,
            STREAM_KAFKA_SSL_CLIENT_CERTIFICATE, STREAM_KAFKA_SSL_CLIENT_KEY,
            STREAM_KAFKA_SSL_CLIENT_KEY_ALGORITHM, SSL_KEYSTORE_TYPE);
      }
    }
    if (StringUtils.isAnyEmpty(trustStoreLocation, trustStorePassword, serverCertificate)) {
      LOGGER.info("Skipping auto SSL server validation since it's not configured.");
      return;
    }
    validateAutoSslMaterial(consumerProps);
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

  private static void validateAutoSslProperties(Properties consumerProps, String... propertyNames) {
    for (String propertyName : propertyNames) {
      String value = consumerProps.getProperty(propertyName);
      if (value != null && ConfigTransformer.DEFAULT_PATTERN.matcher(value).find()) {
        throw new IllegalArgumentException("Kafka ConfigProvider references are not supported for '" + propertyName
            + "' when Pinot auto-generates Kafka SSL stores; use a prebuilt keystore or truststore file instead");
      }
    }
  }

  private static void validateAutoSslMaterial(Properties consumerProps) {
    try {
      Paths.get(consumerProps.getProperty(SSL_TRUSTSTORE_LOCATION));
      String certificateType = consumerProps.getProperty(STREAM_KAFKA_SSL_CERTIFICATE_TYPE, DEFAULT_CERTIFICATE_TYPE);
      CertificateFactory certificateFactory = CertificateFactory.getInstance(certificateType);
      certificateFactory.generateCertificate(new ByteArrayInputStream(
          Base64.getDecoder().decode(consumerProps.getProperty(STREAM_KAFKA_SSL_SERVER_CERTIFICATE))));
      KeyStore.getInstance(consumerProps.getProperty(SSL_TRUSTSTORE_TYPE, DEFAULT_TRUSTSTORE_TYPE));

      String keyStoreLocation = consumerProps.getProperty(SSL_KEYSTORE_LOCATION);
      String keyStorePassword = consumerProps.getProperty(SSL_KEYSTORE_PASSWORD);
      String keyPassword = consumerProps.getProperty(SSL_KEY_PASSWORD);
      String clientCertificate = consumerProps.getProperty(STREAM_KAFKA_SSL_CLIENT_CERTIFICATE);
      if (StringUtils.isAnyEmpty(keyStoreLocation, keyStorePassword, keyPassword, clientCertificate)) {
        return;
      }

      Paths.get(keyStoreLocation);
      Certificate parsedClientCertificate = certificateFactory.generateCertificate(
          new ByteArrayInputStream(Base64.getDecoder().decode(clientCertificate)));
      KeyStore.getInstance(consumerProps.getProperty(SSL_KEYSTORE_TYPE, DEFAULT_KEYSTORE_TYPE));
      byte[] privateKey = Base64.getDecoder().decode(consumerProps.getProperty(STREAM_KAFKA_SSL_CLIENT_KEY));
      String privateKeyAlgorithm = consumerProps.getProperty(STREAM_KAFKA_SSL_CLIENT_KEY_ALGORITHM,
          DEFAULT_KEY_ALGORITHM);
      PrivateKey parsedPrivateKey =
          KeyFactory.getInstance(privateKeyAlgorithm).generatePrivate(new PKCS8EncodedKeySpec(privateKey));
      validatePrivateKeyMatchesPublicKey(parsedPrivateKey, parsedClientCertificate.getPublicKey());
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid configuration for Pinot-generated Kafka SSL stores", e);
    }
  }

  @VisibleForTesting
  static void initTrustStore(Properties consumerProps) {
    Path trustStorePath = getTrustStorePath(consumerProps);
    LOGGER.info("Initializing the SSL trust store");
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

      writeKeyStoreAtomically(trustStorePath, trustStore, trustStorePassword);
      LOGGER.info("Initialized the SSL trust store.");
    } catch (Exception ex) {
      throw new RuntimeException("Error initializing the SSL trust store", ex);
    }
  }

  @VisibleForTesting
  static void initKeyStore(Properties consumerProps) {
    Path keyStorePath = getKeyStorePath(consumerProps);
    LOGGER.info("Initializing the SSL key store");
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
      validatePrivateKeyMatchesPublicKey(privateKey, certificate.getPublicKey());

      // Create a KeyStore object and load a new empty keystore
      KeyStore keyStore = KeyStore.getInstance(keyStoreType);
      keyStore.load(null, null);

      // Add the key pair and certificate to the keystore
      KeyStore.PrivateKeyEntry privateKeyEntry = new KeyStore.PrivateKeyEntry(
          privateKey, new Certificate[]{certificate}
      );
      KeyStore.PasswordProtection keyPasswordProtection = new KeyStore.PasswordProtection(keyPassword.toCharArray());
      keyStore.setEntry(DEFAULT_CLIENT_ALIAS, privateKeyEntry, keyPasswordProtection);

      writeKeyStoreAtomically(keyStorePath, keyStore, keyStorePassword);
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

  private static void writeKeyStoreAtomically(Path storePath, KeyStore keyStore, String password)
      throws Exception {
    Path absoluteStorePath = storePath.toAbsolutePath();
    Path parentDirectory = absoluteStorePath.getParent();
    Files.createDirectories(parentDirectory);
    Path temporaryStorePath = Files.createTempFile(parentDirectory, "pinot-kafka-ssl-", ".tmp");
    try {
      try (OutputStream outputStream = Files.newOutputStream(temporaryStorePath)) {
        keyStore.store(outputStream, password.toCharArray());
      }
      try {
        Files.move(temporaryStorePath, absoluteStorePath, StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        throw new IOException("Atomic replacement is not supported for Kafka SSL store: " + absoluteStorePath, e);
      }
    } finally {
      Files.deleteIfExists(temporaryStorePath);
    }
  }

  @VisibleForTesting
  static void validatePrivateKeyMatchesPublicKey(PrivateKey privateKey, PublicKey publicKey)
      throws Exception {
    String privateKeyFamily = getKeyFamily(privateKey.getAlgorithm());
    String publicKeyFamily = getKeyFamily(publicKey.getAlgorithm());
    if (!privateKeyFamily.equals(publicKeyFamily)) {
      throw new IllegalArgumentException("Kafka SSL client private key algorithm does not match the certificate");
    }

    String signatureAlgorithm;
    PSSParameterSpec pssParameterSpec = null;
    if (privateKey instanceof EdECKey) {
      signatureAlgorithm = ((EdECKey) privateKey).getParams().getName();
    } else if (privateKey.getAlgorithm().equalsIgnoreCase("RSASSA-PSS")
        || publicKey.getAlgorithm().equalsIgnoreCase("RSASSA-PSS")) {
      signatureAlgorithm = "RSASSA-PSS";
      pssParameterSpec = getPssParameterSpec(privateKey, publicKey);
    } else {
      switch (privateKey.getAlgorithm().toUpperCase(Locale.ROOT)) {
        case "RSA":
          signatureAlgorithm = "SHA256withRSA";
          break;
        case "EC":
          signatureAlgorithm = "SHA256withECDSA";
          break;
        case "DSA":
          int qSize = ((DSAKey) privateKey).getParams().getQ().bitLength();
          signatureAlgorithm = qSize <= 160 ? "SHA1withDSA" : qSize <= 224 ? "SHA224withDSA" : "SHA256withDSA";
          break;
        default:
          throw new IllegalArgumentException("Unsupported Kafka SSL client private key algorithm: "
              + privateKey.getAlgorithm());
      }
    }

    byte[] proof = "pinot-kafka-ssl-key-check".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    Signature signature = Signature.getInstance(signatureAlgorithm);
    if (pssParameterSpec != null) {
      signature.setParameter(pssParameterSpec);
    }
    signature.initSign(privateKey);
    signature.update(proof);
    byte[] signedProof = signature.sign();
    signature.initVerify(publicKey);
    signature.update(proof);
    if (!signature.verify(signedProof)) {
      throw new IllegalArgumentException("Kafka SSL client private key does not match the certificate");
    }
  }

  private static String getKeyFamily(String algorithm) {
    String normalizedAlgorithm = algorithm.toUpperCase(Locale.ROOT);
    if (normalizedAlgorithm.equals("RSA") || normalizedAlgorithm.equals("RSASSA-PSS")) {
      return "RSA";
    }
    if (normalizedAlgorithm.equals("EDDSA") || normalizedAlgorithm.equals("ED25519")
        || normalizedAlgorithm.equals("ED448")) {
      return "EDDSA";
    }
    return normalizedAlgorithm;
  }

  private static PSSParameterSpec getPssParameterSpec(PrivateKey privateKey, PublicKey publicKey) {
    AlgorithmParameterSpec privateKeyParameters =
        privateKey instanceof RSAKey ? ((RSAKey) privateKey).getParams() : null;
    if (privateKeyParameters instanceof PSSParameterSpec) {
      return (PSSParameterSpec) privateKeyParameters;
    }
    AlgorithmParameterSpec publicKeyParameters =
        publicKey instanceof RSAKey ? ((RSAKey) publicKey).getParams() : null;
    if (publicKeyParameters instanceof PSSParameterSpec) {
      return (PSSParameterSpec) publicKeyParameters;
    }
    return new PSSParameterSpec("SHA-256", "MGF1", MGF1ParameterSpec.SHA256, 32, 1);
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
}
