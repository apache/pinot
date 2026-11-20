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
package org.apache.pinot.client;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Dsl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Real TLS handshakes through [DefaultSslContextProvider] against an in-process HTTPS server.
///
/// The server certificate (`tls/localhost-only.p12`, password `changeit`) is self-signed with `SAN=dns:localhost` and
/// no IP SAN, and the client trusts exactly that certificate, so only hostname verification can reject a connection.
/// It was generated with:
/// `keytool -genkeypair -alias localhost -keyalg RSA -keysize 2048 -sigalg SHA256withRSA -dname CN=localhost
/// -ext SAN=dns:localhost -validity 36500 -storetype PKCS12 -keystore localhost-only.p12 -storepass changeit`
public class DefaultSslContextProviderHandshakeTest {
  private static final String KEYSTORE_RESOURCE = "tls/localhost-only.p12";
  private static final char[] KEYSTORE_PASSWORD = "changeit".toCharArray();

  private HttpsServer _server;
  private SSLContext _clientSslContext;

  @BeforeClass
  public void setUp()
      throws Exception {
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(KEYSTORE_RESOURCE)) {
      keyStore.load(inputStream, KEYSTORE_PASSWORD);
    }
    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, KEYSTORE_PASSWORD);
    SSLContext serverSslContext = SSLContext.getInstance("TLS");
    serverSslContext.init(keyManagerFactory.getKeyManagers(), null, null);

    // Bind the wildcard address so both "localhost" (IPv4 or IPv6) and "127.0.0.1" reach the server.
    _server = HttpsServer.create(new InetSocketAddress(0), 0);
    _server.setHttpsConfigurator(new HttpsConfigurator(serverSslContext));
    _server.createContext("/", exchange -> {
      byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream outputStream = exchange.getResponseBody()) {
        outputStream.write(body);
      }
    });
    _server.start();

    TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(keyStore);
    _clientSslContext = SSLContext.getInstance("TLS");
    _clientSslContext.init(null, trustManagerFactory.getTrustManagers(), null);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    if (_server != null) {
      _server.stop(0);
    }
  }

  @Test
  public void testMatchingHostnameSucceeds()
      throws Exception {
    assertEquals(getStatusCode(Dsl.config(), "localhost"), 200);
  }

  @Test
  public void testMismatchedHostnameFails() {
    ExecutionException e = expectThrows(ExecutionException.class, () -> getStatusCode(Dsl.config(), "127.0.0.1"));
    assertTrue(ExceptionUtils.indexOfThrowable(e, CertificateException.class) >= 0, ExceptionUtils.getStackTrace(e));
  }

  @Test
  public void testDisabledEndpointIdentificationAcceptsMismatchedHostname()
      throws Exception {
    DefaultAsyncHttpClientConfig.Builder builder = Dsl.config().setDisableHttpsEndpointIdentificationAlgorithm(true);
    assertEquals(getStatusCode(builder, "127.0.0.1"), 200);
  }

  private int getStatusCode(DefaultAsyncHttpClientConfig.Builder builder, String host)
      throws Exception {
    new DefaultSslContextProvider().configure(builder, _clientSslContext, TlsProtocols.defaultProtocols(false));
    try (AsyncHttpClient client = Dsl.asyncHttpClient(builder.build())) {
      return client.prepareGet("https://" + host + ":" + _server.getAddress().getPort() + "/")
          .execute()
          .get(30, TimeUnit.SECONDS)
          .getStatusCode();
    }
  }
}
