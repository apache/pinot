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
package org.apache.pinot.core.util;

import com.google.common.base.Preconditions;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.SecureProtocolSocketFactory;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.core.transport.TlsConfig;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Utility class for shared TLS configuration logic
 */
public final class TlsUtils {
  private static final String ENABLED = "enabled";
  private static final String CLIENT_AUTH_ENABLED = "client.auth.enabled";
  private static final String KEYSTORE_PATH = "keystore.path";
  private static final String KEYSTORE_PASSWORD = "keystore.password";
  private static final String TRUSTSTORE_PATH = "truststore.path";
  private static final String TRUSTSTORE_PASSWORD = "truststore.password";

  private TlsUtils() {
    // left blank
  }

  /**
   * Extract a TlsConfig instance from a namespaced set of configuration key, with optional defaults.
   *
   * @param tlsDefaults default values for TLS settings
   * @param pinotConfig pinot configuration
   * @param namespace namespace prefix
   *
   * @return TlsConfig instance
   */
  public static TlsConfig extractTlsConfig(TlsConfig tlsDefaults, PinotConfiguration pinotConfig, String namespace) {
    TlsConfig tlsConfig = new TlsConfig();
    tlsConfig.setEnabled(tlsDefaults.isEnabled());
    tlsConfig.setClientAuthEnabled(tlsDefaults.isClientAuthEnabled());
    tlsConfig.setKeyStorePath(tlsDefaults.getKeyStorePath());
    tlsConfig.setKeyStorePassword(tlsDefaults.getKeyStorePassword());
    tlsConfig.setTrustStorePath(tlsDefaults.getTrustStorePath());
    tlsConfig.setTrustStorePassword(tlsDefaults.getTrustStorePassword());

    if (pinotConfig.containsKey(key(namespace, ENABLED))) {
      tlsConfig.setEnabled(pinotConfig.getProperty(key(namespace, ENABLED), false));
    }

    if (pinotConfig.containsKey(key(namespace, CLIENT_AUTH_ENABLED))) {
      tlsConfig.setClientAuthEnabled(pinotConfig.getProperty(key(namespace, CLIENT_AUTH_ENABLED), false));
    }

    if (pinotConfig.containsKey(key(namespace, KEYSTORE_PATH))) {
      tlsConfig.setKeyStorePath(pinotConfig.getProperty(key(namespace, KEYSTORE_PATH)));
    }

    if (pinotConfig.containsKey(key(namespace, KEYSTORE_PASSWORD))) {
      tlsConfig.setKeyStorePassword(pinotConfig.getProperty(key(namespace, KEYSTORE_PASSWORD)));
    }

    if (pinotConfig.containsKey(key(namespace, TRUSTSTORE_PATH))) {
      tlsConfig.setTrustStorePath(pinotConfig.getProperty(key(namespace, TRUSTSTORE_PATH)));
    }

    if (pinotConfig.containsKey(key(namespace, TRUSTSTORE_PASSWORD))) {
      tlsConfig.setTrustStorePassword(pinotConfig.getProperty(key(namespace, TRUSTSTORE_PASSWORD)));
    }

    return tlsConfig;
  }

  /**
   * Extract a TlsConfig instance from a namespaced set of configuration keys.
   *
   * @param pinotConfig pinot configuration
   * @param namespace namespace prefix
   *
   * @return TlsConfig instance
   */
  public static TlsConfig extractTlsConfig(PinotConfiguration pinotConfig, String namespace) {
    return extractTlsConfig(new TlsConfig(), pinotConfig, namespace);
  }

  /**
   * Create a KeyManagerFactory instance from a given TlsConfig.
   *
   * @param tlsConfig TLS config
   *
   * @return KeyManagerFactory
   */
  public static KeyManagerFactory createKeyManagerFactory(TlsConfig tlsConfig) {
    Preconditions.checkArgument(tlsConfig.isEnabled(), "tls is disabled");
    Preconditions.checkNotNull(tlsConfig.getKeyStorePath(), "key store path is null");
    Preconditions.checkNotNull(tlsConfig.getKeyStorePassword(), "key store password is null");

    try {
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      try (FileInputStream is = new FileInputStream(tlsConfig.getKeyStorePath())) {
        keyStore.load(is, tlsConfig.getKeyStorePassword().toCharArray());
      }

      KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagerFactory.init(keyStore, tlsConfig.getKeyStorePassword().toCharArray());

      return keyManagerFactory;

    } catch (Exception e) {
      throw new RuntimeException(String.format("Could not create key manager factory '%s'",
          tlsConfig.getKeyStorePath()), e);
    }
  }

  /**
   * Create a TrustManagerFactory instance from a given TlsConfig.
   *
   * @param tlsConfig TLS config
   *
   * @return TrustManagerFactory
   */
  public static TrustManagerFactory createTrustManagerFactory(TlsConfig tlsConfig) {
    Preconditions.checkArgument(tlsConfig.isEnabled(), "tls is disabled");
    Preconditions.checkNotNull(tlsConfig.getTrustStorePath(), "trust store path is null");
    Preconditions.checkNotNull(tlsConfig.getTrustStorePassword(), "trust store password is null");

    try {
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      try (FileInputStream is = new FileInputStream(tlsConfig.getTrustStorePath())) {
        keyStore.load(is, tlsConfig.getTrustStorePassword().toCharArray());
      }

      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(keyStore);

      return trustManagerFactory;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Could not create trust manager factory '%s'",
          tlsConfig.getTrustStorePath()), e);
    }
  }

  /**
   * Installs a default TLS socket factory for all HttpsURLConnection instances based on a given TlsConfig (1 or 2-way)
   *
   * @param tlsConfig TLS config
   */
  public static void installDefaultSSLSocketFactory(TlsConfig tlsConfig) {
    KeyManager[] keyManagers = null;
    if (tlsConfig.getKeyStorePath() != null) {
      keyManagers = createKeyManagerFactory(tlsConfig).getKeyManagers();
    }

    TrustManager[] trustManagers = null;
    if (tlsConfig.getTrustStorePath() != null) {
      trustManagers = createTrustManagerFactory(tlsConfig).getTrustManagers();
    }

    try {
      SSLContext sc = SSLContext.getInstance("SSL");
      sc.init(keyManagers, trustManagers, new java.security.SecureRandom());

      // HttpsURLConnection
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

      // Apache HTTP client 3.x
      Protocol.registerProtocol("https", new Protocol(CommonConstants.HTTPS_PROTOCOL,
          new CustomApacheHttpSocketFactory(sc.getSocketFactory()), 443));

    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Could not initialize SSL support", e);
    }
  }

  private static String key(String namespace, String suffix) {
    return namespace + "." + suffix;
  }

  /**
   * Adapted from: https://svn.apache.org/viewvc/httpcomponents/oac.hc3x/trunk/src/contrib/org/apache/commons/httpclient/contrib/ssl/AuthSSLProtocolSocketFactory.java?view=markup
   */
  private static class CustomApacheHttpSocketFactory implements SecureProtocolSocketFactory {
    final SSLSocketFactory _sslSocketFactory;

    public CustomApacheHttpSocketFactory(SSLSocketFactory sslSocketFactory) {
      _sslSocketFactory = sslSocketFactory;
    }

    @Override
    public Socket createSocket(Socket socket, String host, int port, boolean autoClose)
        throws IOException, UnknownHostException {
      return _sslSocketFactory.createSocket(socket, host, port, autoClose);
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localAddress, int localPort)
        throws IOException, UnknownHostException {
      return _sslSocketFactory.createSocket(host, port, localAddress, localPort);
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localAddress, int localPort,
        HttpConnectionParams params)
        throws IOException, UnknownHostException, ConnectTimeoutException {
      Preconditions.checkNotNull(params);

      int timeout = params.getConnectionTimeout();
      if (timeout <= 0) {
        return _sslSocketFactory.createSocket(host, port, localAddress, localPort);
      }

      Socket socket = _sslSocketFactory.createSocket();
      SocketAddress localaddr = new InetSocketAddress(localAddress, localPort);
      SocketAddress remoteaddr = new InetSocketAddress(host, port);
      socket.bind(localaddr);
      socket.connect(remoteaddr, timeout);
      return socket;
    }

    @Override
    public Socket createSocket(String host, int port)
        throws IOException, UnknownHostException {
      return _sslSocketFactory.createSocket(host, port);
    }
  }
}
