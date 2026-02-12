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
package org.apache.pinot.common.auth.vault;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Vault utility class for fetching LDAP credentials with self-healing retry mechanism.
 * Based on working Trino VaultUtil implementation.
 */
public final class VaultUtil {
  public static final Logger log = LoggerFactory.getLogger(VaultUtil.class);

  static String _ioExceptionPostBasicAuth = "IOException in postBasicAuth";
  static String _malformedURLExceptioninPostBasicAuth = "MalformedURLException in postBasicAuth";
  public static final String MAXIMUM_RETRY_LIMIT = "10";
  public static final String MAXIMUM_RETRY_INTERVAL = "300";

  private VaultUtil() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }

  /**
   * Check if the string passed in is a valid number.
   */
  static boolean isNumber(String str) {
    if (str == null || str.length() == 0) {
      return false;
    }
    return str.chars().allMatch(Character::isDigit);
  }

  /**
   * Check if the number passed as string is positive ( greater than 0 ).
   * Inherently checks if it is a number by calling isNumber().
   */
  public static boolean isPositive(String str) {
    return isNumber(str) && Integer.parseInt(str) > 0;
  }

  /**
   * Self-healing retry mechanism for vault operations.
   */
  public static Map<String, String> selfHealingRetry(VaultConfig conf)
      throws InterruptedException {

    int retryLimit = determineRetryLimit(conf);
    int retryInterval = Integer.parseInt(conf.getVaultRetryInterval());

    String token = null;

    for (int attempt = 1; attempt <= retryLimit; attempt++) {
      try {
        // Generate or regenerate token
        if (token == null) {
          log.info("Attempting to get vault token (attempt {}/{})", attempt, retryLimit);
          token = tokenCall(conf);
        }

        log.info("Attempting to fetch vault data (attempt {}/{})", attempt, retryLimit);
        Map<String, String> data = secureVaultCall(conf, token);

        if (data != null && !data.isEmpty()) {
          log.info("Successfully fetched vault data on attempt {}", attempt);
          return data;
        }
        log.warn("Vault returned empty data on attempt {}", attempt);
      } catch (Exception e) {
        log.error("Error fetching vault data on attempt " + attempt + "/" + retryLimit, e);
        token = null; // Force token regeneration on error

        if (attempt < retryLimit) {
          log.info("Retrying in {} seconds...", retryInterval);
          try {
            Thread.sleep(retryInterval * 1000L);
          } catch (InterruptedException ie) {
            log.error("Interrupted while waiting to retry", ie);
            Thread.currentThread().interrupt();
            throw ie;
          }
        }
      }
    }

    // All retries exhausted
    String errorMsg = String.format(
        "Failed to fetch vault data after %d attempts",
        retryLimit);
    log.error(errorMsg);
    throw new RuntimeException(errorMsg);
  }

  private static int determineRetryLimit(VaultConfig conf) {
    String configuredLimit = conf.getVaultRetryLimit();
    if (isPositive(configuredLimit)) {
      int limit = Integer.parseInt(configuredLimit);
      int maxLimit = Integer.parseInt(MAXIMUM_RETRY_LIMIT);
      return Math.min(limit, maxLimit);
    }
    return Integer.parseInt(MAXIMUM_RETRY_LIMIT);
  }

  private static String tokenCall(VaultConfig conf)
      throws Exception {
      try {
          URL url = new URL(conf.getVaultBaseUrl() + "auth/cert/login");

          String respstring = secureConnRespMessage(conf, url, null);
          VaultResponse resp = new Gson().fromJson(respstring, VaultResponse.class);

          return resp.getAuth().getClientToken();
      } catch (MalformedURLException e) {
          log.error(_malformedURLExceptioninPostBasicAuth);
          throw new RuntimeException(e);
      } catch (IOException e) {
          log.error(_ioExceptionPostBasicAuth);
          throw new RuntimeException(e);
      } catch (UnrecoverableKeyException e) {
          throw new RuntimeException(e);
      } catch (CertificateException e) {
          throw new RuntimeException(e);
      } catch (NoSuchAlgorithmException e) {
          throw new RuntimeException(e);
      } catch (KeyStoreException e) {
          throw new RuntimeException(e);
      } catch (KeyManagementException e) {
          throw new RuntimeException(e);
      }
  }

  private static Map<String, String> secureVaultCall(VaultConfig conf, String token)
      throws Exception {
      try {
          URL finalurl = new URL(conf.getVaultBaseUrl() + conf.getVaultPath());

          String respstring = secureConnRespMessage(conf, finalurl, token);

          VaultResponse resp = new Gson().fromJson(
                  respstring, VaultResponse.class);

          if (resp.getErrors() != null) {
              throw new Exception("Processing failed for Vault secure call");
          }

          return resp.getData();
      } catch (MalformedURLException e) {
          log.error(_malformedURLExceptioninPostBasicAuth);
          throw new Exception(e);
      } catch (IOException e) {
          log.error(_ioExceptionPostBasicAuth);
          throw new Exception(e);
      } catch (UnrecoverableKeyException | CertificateException | NoSuchAlgorithmException | KeyStoreException
             | KeyManagementException e) {
          throw new Exception(e);
      }
  }

  private static X509Certificate loadCertificate(File certificateFile)
      throws IOException, CertificateException {
    try (FileInputStream inputStream = new FileInputStream(certificateFile)) {
      return (X509Certificate) CertificateFactory.getInstance("X509").generateCertificate(inputStream);
    }
  }

  private static String secureConnRespMessage(VaultConfig conf, URL finalurl, String token)
      throws Exception {
    try {
        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        String storename;
        char[] storepass;
        storename = conf.getVaultCert();
        storepass = conf.getVaultCertKey().toCharArray();
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("accmvault",
                loadCertificate(new File(conf.getVaultCaCert())));
        tmf.init(trustStore);
        try (FileInputStream fin = new FileInputStream(storename)) {
            ks.load(fin, storepass);
        }
        kmf.init(ks, storepass);
        java.util.Arrays.fill(storepass, ' ');
        TrustManager[] trustManagers = tmf.getTrustManagers();
        // initialized sslContext to trust all server certificates and pass our
        // certificate for client authentication
        sslContext.init(kmf.getKeyManagers(), trustManagers, null);
        // set the socket factory to be used to during HTTPS call
        HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
        HttpsURLConnection connection = (HttpsURLConnection) finalurl.openConnection();
        if (token == null) {
            connection.setSSLSocketFactory(sslContext.getSocketFactory());
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
        } else {
            connection.setSSLSocketFactory(sslContext.getSocketFactory());
            connection.setDoOutput(true);
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("X-Vault-Token", token);
        }

        connection.connect();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                connection.getResponseCode() / 100 == 2 ? connection.getInputStream()
                        : connection.getErrorStream()))) {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            if (line != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    } catch (MalformedURLException e) {
      log.error(_malformedURLExceptioninPostBasicAuth);
      throw new Exception(e);
    } catch (IOException e) {
      log.error(_ioExceptionPostBasicAuth);
      throw new Exception(e);
    } catch (UnrecoverableKeyException | CertificateException | NoSuchAlgorithmException | KeyStoreException
             | KeyManagementException e) {
      throw new Exception(e);
    }
  }
}
