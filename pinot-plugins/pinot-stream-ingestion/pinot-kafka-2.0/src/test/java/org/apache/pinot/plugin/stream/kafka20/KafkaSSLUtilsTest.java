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

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;
import java.util.UUID;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.util.encoders.Base64;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class KafkaSSLUtilsTest {
  private String _trustStorePath;
  private String _keyStorePath;
  private static final String DEFAULT_TRUSTSTORE_PASSWORD = "mytruststorepassword";
  private static final String DEFAULT_KEYSTORE_PASSWORD = "mykeystorepassword";

  static {
    // helps generate the X509Certificate
    Security.addProvider(new BouncyCastleProvider());
  }

  @BeforeMethod
  private void setup() {
    _trustStorePath = "/tmp/" + UUID.randomUUID() + "/client.truststore.jks";
    _keyStorePath = "/tmp/" + UUID.randomUUID() + "/client.keystore.p12";
  }

  @AfterMethod
  private void cleanup() {
    Path trustStorePath = Paths.get(_trustStorePath);
    try {
      Files.deleteIfExists(trustStorePath);
    } catch (IOException ex) {
      // ignored
    }

    Path keyStorePath = Paths.get(_keyStorePath);
    try {
      Files.deleteIfExists(keyStorePath);
    } catch (IOException ex) {
      // ignored
    }
  }

  @Test
  public void testInitTrustStore()
      throws CertificateException, NoSuchAlgorithmException, OperatorCreationException, NoSuchProviderException,
             IOException, KeyStoreException {
    Properties consumerProps = new Properties();
    setTrustStoreProps(consumerProps);

    // should not throw any exceptions
    KafkaSSLUtils.initTrustStore(consumerProps);
    validateTrustStoreCertificateCount(1);
  }

  @Test
  public void testInitKeyStore()
      throws CertificateException, NoSuchAlgorithmException, OperatorCreationException, NoSuchProviderException,
             IOException, KeyStoreException {
    Properties consumerProps = new Properties();
    setKeyStoreProps(consumerProps);

    // should not throw any exceptions
    KafkaSSLUtils.initKeyStore(consumerProps);
    validateKeyStoreCertificateCount(1);
  }

  @Test
  public void testInitSSLTrustStoreAndKeyStore()
      throws CertificateException, NoSuchAlgorithmException, OperatorCreationException, NoSuchProviderException,
             KeyStoreException, IOException {
    Properties consumerProps = new Properties();
    setTrustStoreProps(consumerProps);
    setKeyStoreProps(consumerProps);

    // should not throw any exceptions
    KafkaSSLUtils.initSSL(consumerProps);

    // validate
    validateTrustStoreCertificateCount(1);
    validateKeyStoreCertificateCount(1);
  }

  @Test
  public void testInitSSLTrustStoreOnly()
      throws CertificateException, NoSuchAlgorithmException, OperatorCreationException, NoSuchProviderException,
             IOException, KeyStoreException {
    Properties consumerProps = new Properties();
    setTrustStoreProps(consumerProps);

    // should not throw any exceptions
    KafkaSSLUtils.initSSL(consumerProps);

    // validate
    validateTrustStoreCertificateCount(1);
  }

  @Test (expectedExceptions = java.io.FileNotFoundException.class)
  public void testInitSSLKeyStoreOnly()
      throws CertificateException, NoSuchAlgorithmException, OperatorCreationException, NoSuchProviderException,
             IOException, KeyStoreException {
    Properties consumerProps = new Properties();
    setKeyStoreProps(consumerProps);

    // should not throw any exceptions
    KafkaSSLUtils.initSSL(consumerProps);

    // Validate that no certificates are installed
    validateTrustStoreCertificateCount(0);
  }

  @Test
  public void testInitSSLAndRenewCertificates()
      throws CertificateException, NoSuchAlgorithmException, OperatorCreationException, NoSuchProviderException,
             IOException, KeyStoreException {
    Properties consumerProps = new Properties();
    setTrustStoreProps(consumerProps);
    setKeyStoreProps(consumerProps);
    KafkaSSLUtils.initSSL(consumerProps);

    // renew the truststore and keystore
    setTrustStoreProps(consumerProps);
    setKeyStoreProps(consumerProps);
    KafkaSSLUtils.initSSL(consumerProps);

    // validate
    validateTrustStoreCertificateCount(1);
    validateKeyStoreCertificateCount(1);
  }

  @Test
  public void testInitSSLBackwardsCompatibilityCheck()
      throws CertificateException, NoSuchAlgorithmException, OperatorCreationException, NoSuchProviderException,
             IOException, KeyStoreException {
    Properties consumerProps = new Properties();
    setTrustStoreProps(consumerProps);
    setKeyStoreProps(consumerProps);
    KafkaSSLUtils.initSSL(consumerProps);

    // validate
    validateTrustStoreCertificateCount(1);
    validateKeyStoreCertificateCount(1);

    setTrustStoreProps(consumerProps); // new server certificate is generated
    consumerProps.remove("stream.kafka.ssl.server.certificate");
    setKeyStoreProps(consumerProps); // new client certificate is generated
    consumerProps.remove("stream.kafka.ssl.client.certificate");

    // Attempt to initialize the trust store and key store again without passing the required certificates
    KafkaSSLUtils.initSSL(consumerProps);
    // validate again that the existing certificates are untouched.
    validateTrustStoreCertificateCount(1);
    validateKeyStoreCertificateCount(1);
  }

  private void validateTrustStoreCertificateCount(int expCount)
      throws CertificateException, IOException, NoSuchAlgorithmException, KeyStoreException {
    // Validate that certificate is installed in the trust store
    KeyStore trustStore = KeyStore.getInstance("JKS");
    try (FileInputStream fis = new FileInputStream(_trustStorePath)) {
      trustStore.load(fis, DEFAULT_TRUSTSTORE_PASSWORD.toCharArray());
    }

    int certCount = 0;
    // Iterate through the aliases in the TrustStore
    Enumeration<String> aliases = trustStore.aliases();
    while (aliases.hasMoreElements()) {
      String alias = aliases.nextElement();
      // Check if the alias refers to a certificate
      if (trustStore.isCertificateEntry(alias)) {
        ++certCount;
      }
    }
    Assert.assertEquals(expCount, certCount);
  }

  private void validateKeyStoreCertificateCount(int expCount)
      throws CertificateException, IOException, NoSuchAlgorithmException, KeyStoreException {
    // Validate that certificate is installed in the key store
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    try (FileInputStream fis = new FileInputStream(_keyStorePath)) {
      keyStore.load(fis, DEFAULT_KEYSTORE_PASSWORD.toCharArray());
    }

    int certCount = 0;
    // Iterate through the aliases in the TrustStore
    Enumeration<String> aliases = keyStore.aliases();
    while (aliases.hasMoreElements()) {
      String alias = aliases.nextElement();
      // Check if the alias refers to a key
      if (keyStore.isKeyEntry(alias)) {
        ++certCount;
      }
    }
    Assert.assertEquals(expCount, certCount);
  }

  private void setTrustStoreProps(Properties consumerProps)
      throws CertificateException, NoSuchAlgorithmException, OperatorCreationException, NoSuchProviderException {
    String[] certCreationResult = generateSelfSignedCertificate();
    String serverCertificate = certCreationResult[1];
    consumerProps.setProperty("stream.kafka.ssl.server.certificate", serverCertificate);
    consumerProps.setProperty("stream.kafka.ssl.server.certificate.type", "X.509");
    consumerProps.setProperty("ssl.truststore.type", "jks");
    consumerProps.setProperty("ssl.truststore.location", _trustStorePath);
    consumerProps.setProperty("ssl.truststore.password", DEFAULT_TRUSTSTORE_PASSWORD);
  }

  private void setKeyStoreProps(Properties consumerProps)
      throws CertificateException, NoSuchAlgorithmException, OperatorCreationException, NoSuchProviderException {
    String[] certCreationResult = generateSelfSignedCertificate();
    String privateKey = certCreationResult[0];
    String clientCertificate = certCreationResult[1];
    consumerProps.setProperty("ssl.keystore.location", _keyStorePath);
    consumerProps.setProperty("ssl.keystore.password", DEFAULT_KEYSTORE_PASSWORD);
    consumerProps.setProperty("ssl.keystore.type", "PKCS12");
    consumerProps.setProperty("ssl.key.password", "mykeypwd");
    consumerProps.setProperty("stream.kafka.ssl.certificate.type", "X.509");
    consumerProps.setProperty("stream.kafka.ssl.client.certificate", clientCertificate);
    consumerProps.setProperty("stream.kafka.ssl.client.key", privateKey);
    consumerProps.setProperty("stream.kafka.ssl.client.key.algorithm", "RSA");
  }

  private String[] generateSelfSignedCertificate()
      throws CertificateException, OperatorCreationException, NoSuchAlgorithmException, NoSuchProviderException {
    String[] certCreationResult = new String[2];
    // Generate a key pair
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", "BC");
    keyPairGenerator.initialize(2048, new SecureRandom());
    KeyPair keyPair = keyPairGenerator.generateKeyPair();
    // set the private key into the result object
    certCreationResult[0] = Base64.toBase64String(keyPair.getPrivate().getEncoded());

    // Validity of the certificate
    Date notBefore = new Date();
    Date notAfter = new Date(notBefore.getTime() + 7 * 24 * 60 * 60 * 1000L); // 1 week

    // Issuer and Subject DN
    X500Name issuerName = new X500Name("CN=Test CA, O=Eng, OU=IT, L=Sunnyvale, ST=CA, C=US");
    X500Name subjectName = new X500Name("CN=Test User, O=Eng, OU=IT, L=Sunnyvale, ST=CA, C=US");

    // Serial Number
    BigInteger serial = BigInteger.valueOf(System.currentTimeMillis());

    // Create the certificate builder
    X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
        issuerName,
        serial,
        notBefore,
        notAfter,
        subjectName,
        keyPair.getPublic());

    // Create a signer
    ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").setProvider("BC").build(keyPair.getPrivate());

    // Build the certificate
    X509Certificate cert = new JcaX509CertificateConverter().setProvider("BC")
        .getCertificate(certBuilder.build(signer));
    // set the encoded certificate string into the result object
    certCreationResult[1] = Base64.toBase64String(cert.getEncoded());
    return certCreationResult;
  }
}
