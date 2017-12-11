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
import com.linkedin.pinot.common.utils.FileUploadUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
 * Note:
 * The 'server' in this class is not pinot-server.
 * SegmentFetchers are used in pinot-controller (to fetch segments from external sources),
 * and pinot-server (to fetch segment from the controller).
 *
 * The 'server' here indicates the server side of the HTTPS connection.
 *
 * Also note that this is ONE of many combinations of algorithms and types that can be used
 * for an Https client.
 *
 * This implementation will verify the server's certificate against a Certifying Authority (CA)
 * if X509 certificate of the CA is configured. Otherwise, all server certificates will be treated
 * as trusted.
 *
 * The implementation provides for optional configuration for key managers (where client certificate
 * is stored). If configured, the client will present its certificate to the server during TLS
 * protocol exchange.
 *
 * At this time, only PKCS12 files are supported for client certificates, and it is assumed that the
 * server is presenting an X509 certificate.
 */
public class HttpsSegmentFetcher implements SegmentFetcher {
  SSLSocketFactory _sslSocketFactory;

  private static final String SECURITY_ALGORITHM = "TLS";
  private static final String CERTIFICATE_TYPE = "X509";
  private static final String KEYSTORE_TYPE = "PKCS12";
  private static final String KEYMANAGER_FACTORY_ALGORITHM = "SunX509";

  private static String CONFIG_OF_SERVER_CA_CERT = "ssl.server.ca-cert";
  private static String CONFIG_OF_CLIENT_PKCS12_FILE = "ssl.client.pkcs12.file";
  private static String CONFIG_OF_CLIENT_PKCS12_PASSWORD = "ssl.client.pkcs12.password";

  public static Logger LOGGER = LoggerFactory.getLogger(HttpsSegmentFetcher.class);

  @Override
  public void init(Configuration configs) {
    try {
      TrustManager[] trustManagers = setupTrustManagers(configs);
      KeyManager[] keyManagers = setupKeyManagers(configs);

      SSLContext sslContext = SSLContext.getInstance(SECURITY_ALGORITHM);
      sslContext.init(keyManagers, trustManagers, null);
      _sslSocketFactory = sslContext.getSocketFactory();
    } catch (Exception e) {
      Utils.rethrowException(e);
    }
  }

  private TrustManager[] setupTrustManagers(Configuration configs)
      throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException {
    // This is the cert authority that validates server's cert, so we need to put it in our
    // trustStore.
    String serverCACertFile = configs.getString(CONFIG_OF_SERVER_CA_CERT);
    if (serverCACertFile == null) {
      LOGGER.warn("Https server CA Certificate file not confugured (" + CONFIG_OF_SERVER_CA_CERT + "). All servers will be trusted!");
      TrustManager[] trustAllCerts = new TrustManager[] {
          new X509TrustManager() {
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
          }
      };
      return trustAllCerts;
    }
    FileInputStream is = new FileInputStream(new File(serverCACertFile));
    CertificateFactory certificateFactory = CertificateFactory.getInstance(CERTIFICATE_TYPE);
    X509Certificate cert = (X509Certificate) certificateFactory.generateCertificate(is);

    String serverKey = "https-server";
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(null);
    trustStore.setCertificateEntry(serverKey, cert);

    TrustManagerFactory tmf = TrustManagerFactory.getInstance(CERTIFICATE_TYPE);
    tmf.init(trustStore);
    return tmf.getTrustManagers();
  }

  private KeyManager[] setupKeyManagers(Configuration configs) {
    try {
      KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
      String keyStoreFile = configs.getString(CONFIG_OF_CLIENT_PKCS12_FILE);
      String keyStorePassword = configs.getString(CONFIG_OF_CLIENT_PKCS12_PASSWORD);
      if (keyStoreFile == null || keyStorePassword == null) {
        LOGGER.info("Either keystore file name (" + CONFIG_OF_CLIENT_PKCS12_FILE + ") or " +
            "keystore password (" + CONFIG_OF_CLIENT_PKCS12_PASSWORD + ") is not configured"
            + ". Client will not present certificate to the server");
        return null;
      }
      keyStore.load(new FileInputStream(new File(keyStoreFile)), keyStorePassword.toCharArray());
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KEYMANAGER_FACTORY_ALGORITHM);
      kmf.init(keyStore, keyStorePassword.toCharArray());
      return kmf.getKeyManagers();
    } catch (Exception e) {
      Utils.rethrowException(e);
    }
    return null;
  }

  @Override
  public void fetchSegmentToLocal(String uri, File tempFile) throws Exception {
    URL url = new URL(uri);
    HttpsURLConnection conn = (HttpsURLConnection)url.openConnection();
    conn.setSSLSocketFactory(_sslSocketFactory);
    int respCode = conn.getResponseCode();
    long contentLength = conn.getContentLength();
    InputStream is = conn.getInputStream();
    try {
      FileUploadUtils.storeFile(uri, tempFile, respCode, contentLength, is);
    } finally {
      is.close();
    }
  }
}