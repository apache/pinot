
/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.common.utils;

import com.linkedin.pinot.common.Utils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Set;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClientSSLContextGenerator {
  private static final String CONFIG_OF_SERVER_CA_CERT = "server.ca-cert";
  private static final String CONFIG_OF_CLIENT_PKCS12_FILE = "client.pkcs12.file";
  private static final String CONFIG_OF_CLIENT_PKCS12_PASSWORD = "client.pkcs12.password";
  private static final String CONFIG_OF_ENABLE_SERVER_VERIFICATION = "server.enable-verification";
  private static final String SECURITY_ALGORITHM = "TLS";
  private static final String CERTIFICATE_TYPE = "X509";
  private static final String KEYSTORE_TYPE = "PKCS12";
  private static final String KEYMANAGER_FACTORY_ALGORITHM = "SunX509";
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientSSLContextGenerator.class);

  private final String _serverCACertFile;
  private final String _keyStoreFile;
  private final String _keyStorePassword;

  public static Set<String> getProtectedConfigKeys() {
    return Collections.singleton(CONFIG_OF_CLIENT_PKCS12_PASSWORD);
  }

  public ClientSSLContextGenerator(Configuration sslConfig) {
    if (sslConfig.getBoolean(CONFIG_OF_ENABLE_SERVER_VERIFICATION)) {
      _serverCACertFile = sslConfig.getString(CONFIG_OF_SERVER_CA_CERT);
    } else {
      _serverCACertFile = null;
      LOGGER.warn("Https Server CA file not configured.. All servers will be trusted!");
    }
    _keyStoreFile = sslConfig.getString(CONFIG_OF_CLIENT_PKCS12_FILE);
    _keyStorePassword = sslConfig.getString(CONFIG_OF_CLIENT_PKCS12_PASSWORD);
    if ((_keyStorePassword == null && _keyStoreFile != null) ||
        (_keyStorePassword != null && _keyStoreFile == null)) {
      throw new IllegalArgumentException("Invalid configuration of keystore file and passowrd");
    }
  }

  public SSLContext generate() {
    SSLContext sslContext = null;
    try {
      TrustManager[] trustManagers = setupTrustManagers();
      KeyManager[] keyManagers = setupKeyManagers();

      sslContext = SSLContext.getInstance(SECURITY_ALGORITHM);
      sslContext.init(keyManagers, trustManagers, null);
    } catch (Exception e) {
      Utils.rethrowException(e);
    }
    return sslContext;
  }

  private TrustManager[] setupTrustManagers()
      throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException {
    // This is the cert authority that validates server's cert, so we need to put it in our
    // trustStore.
    if (_serverCACertFile != null) {
      LOGGER.info("Initializing trust store from {}", _serverCACertFile);
      FileInputStream is = new FileInputStream(new File(_serverCACertFile));
      KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      trustStore.load(null);
      CertificateFactory certificateFactory = CertificateFactory.getInstance(CERTIFICATE_TYPE);
      int i = 0;
      while (is.available() > 0) {
        X509Certificate cert = (X509Certificate) certificateFactory.generateCertificate(is);
        LOGGER.info("Read certificate serial number {} by issuer {} ", cert.getSerialNumber().toString(16), cert.getIssuerDN().toString());

        String serverKey = "https-server-" + i;
        trustStore.setCertificateEntry(serverKey, cert);
        i++;
      }

      TrustManagerFactory tmf = TrustManagerFactory.getInstance(CERTIFICATE_TYPE);
      tmf.init(trustStore);
      LOGGER.info("Successfully initialized trust store");
      return tmf.getTrustManagers();
    }
    // Server verification disabled. Trust all servers
    TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
      @Override
      public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
      }

      @Override
      public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
      }

      @Override
      public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
      }
    }};
    return trustAllCerts;
  }

  private KeyManager[] setupKeyManagers() {
    if (_keyStoreFile == null) {
      return null;
    }
    try {
      KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
      LOGGER.info("Setting up keystore with file {}", _keyStoreFile);
      keyStore.load(new FileInputStream(new File(_keyStoreFile)), _keyStorePassword.toCharArray());
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KEYMANAGER_FACTORY_ALGORITHM);
      kmf.init(keyStore, _keyStorePassword.toCharArray());
      LOGGER.info("Successfully initialized keystore");
      return kmf.getKeyManagers();
    } catch (Exception e) {
      Utils.rethrowException(e);
    }
    return null;
  }
}
