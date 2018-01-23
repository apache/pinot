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

package com.linkedin.pinot.common.segment.fetcher;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
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


/*
 * The following links are useful to understand some of the SSL terminology (key store, trust store, algorithms,
 * architecture, etc.)
 *
 *   https://docs.oracle.com/javase/7/docs/technotes/guides/security/crypto/CryptoSpec.html
 *   https://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html
 *   https://docs.oracle.com/cd/E19509-01/820-3503/6nf1il6ek/index.html
 *
 * The 'server' in this class is not pinot-server, but the server side of the HTTPS connection.
 * SegmentFetchers are used in pinot-controller (to fetch segments from external sources),
 * and pinot-server (to fetch segment from the controller).
 *
 * Also note that this is ONE of many combinations of algorithms and types that can be used
 * for an Https client. Clearly, more configurations are possible, and can be added as needed.
 *
 * This implementation verifies the server's certificate against a Certifying Authority (CA)
 * if verification is turned on AND X509 certificate of the CA is configured.
 *
 * This implementation provides for optional configuration for key managers (where client certificate
 * is stored). If configured, the client will present its certificate to the server during TLS
 * protocol exchange. If a key manager file is not configured, then client will not present any
 * certificate to the server upon connection.
 *
 * At this time, only PKCS12 files are supported for client certificates, and it is assumed that the
 * server is presenting an X509 certificate.
 */
public class HttpsSegmentFetcher extends HttpSegmentFetcher {
  private static final String SECURITY_ALGORITHM = "TLS";
  private static final String CERTIFICATE_TYPE = "X509";
  private static final String KEYSTORE_TYPE = "PKCS12";
  private static final String KEYMANAGER_FACTORY_ALGORITHM = "SunX509";

  private static final String CONFIG_OF_SERVER_CA_CERT = "ssl.server.ca-cert";
  private static final String CONFIG_OF_CLIENT_PKCS12_FILE = "ssl.client.pkcs12.file";
  private static final String CONFIG_OF_CLIENT_PKCS12_PASSWORD = "ssl.client.pkcs12.password";
  private static final String CONFIG_OF_ENABLE_SERVER_VERIFICATION = "ssl.server.enable-verification";

  @Override
  protected void initHttpClient(Configuration configs) {
    try {
      TrustManager[] trustManagers = setupTrustManagers(configs);
      KeyManager[] keyManagers = setupKeyManagers(configs);

      SSLContext sslContext = SSLContext.getInstance(SECURITY_ALGORITHM);
      sslContext.init(keyManagers, trustManagers, null);
      _httpClient = new FileUploadDownloadClient(sslContext);
    } catch (Exception e) {
      Utils.rethrowException(e);
    }
  }

  private TrustManager[] setupTrustManagers(Configuration configs)
      throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException {
    // This is the cert authority that validates server's cert, so we need to put it in our
    // trustStore.
    final String serverCACertFile = configs.getString(CONFIG_OF_SERVER_CA_CERT);
    final boolean enableServerVerifiction = configs.getBoolean(CONFIG_OF_ENABLE_SERVER_VERIFICATION, true);
    final String fileNotConfigured =
        "Https server CA Certificate file not confugured (" + CONFIG_OF_SERVER_CA_CERT + ")";
    if (enableServerVerifiction) {
      if (serverCACertFile == null) {
        throw new RuntimeException(fileNotConfigured);
      }
      _logger.info("Initializing trust store from {}", serverCACertFile);
      FileInputStream is = new FileInputStream(new File(serverCACertFile));
      KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      trustStore.load(null);
      CertificateFactory certificateFactory = CertificateFactory.getInstance(CERTIFICATE_TYPE);
      int i = 0;
      while (is.available() > 0) {
        X509Certificate cert = (X509Certificate) certificateFactory.generateCertificate(is);
        _logger.info("Read certificate serial number {} by issuer {} ", cert.getSerialNumber().toString(16), cert.getIssuerDN().toString());

        String serverKey = "https-server-" + i;
        trustStore.setCertificateEntry(serverKey, cert);
        i++;
      }

      TrustManagerFactory tmf = TrustManagerFactory.getInstance(CERTIFICATE_TYPE);
      tmf.init(trustStore);
      _logger.info("Successfully initialized trust store");
      return tmf.getTrustManagers();
    }
    // Server verification disabled. Trust all servers
    _logger.warn("{}. All servers will be trusted!", fileNotConfigured);
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

  private KeyManager[] setupKeyManagers(Configuration configs) {
    try {
      KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
      String keyStoreFile = configs.getString(CONFIG_OF_CLIENT_PKCS12_FILE);
      String keyStorePassword = configs.getString(CONFIG_OF_CLIENT_PKCS12_PASSWORD);
      if (keyStoreFile == null || keyStorePassword == null) {
        _logger.info("Either keystore file name ({}) or keystore password ({}) is not configured. Client will not present certificates to server.",
            CONFIG_OF_CLIENT_PKCS12_FILE, CONFIG_OF_CLIENT_PKCS12_PASSWORD);
        return null;
      }
      _logger.info("Setting up keystore with file {}", keyStoreFile);
      keyStore.load(new FileInputStream(new File(keyStoreFile)), keyStorePassword.toCharArray());
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KEYMANAGER_FACTORY_ALGORITHM);
      kmf.init(keyStore, keyStorePassword.toCharArray());
      _logger.info("Successfully initialized keystore");
      return kmf.getKeyManagers();
    } catch (Exception e) {
      Utils.rethrowException(e);
    }
    return null;
  }

  @Override
  public Set<String> getProtectedConfigKeys() {
    return Collections.singleton(CONFIG_OF_CLIENT_PKCS12_PASSWORD);
  }

}