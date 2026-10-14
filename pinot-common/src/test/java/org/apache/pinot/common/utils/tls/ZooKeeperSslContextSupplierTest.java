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
package org.apache.pinot.common.utils.tls;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.common.ZKConfig;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Exercises the released ZooKeeper context factory against a trusted certificate with a mismatched peer identity.
@Test(singleThreaded = true)
public class ZooKeeperSslContextSupplierTest {
  private final Map<String, String> _savedProperties = new HashMap<>();

  @BeforeMethod
  public void saveBootstrapProperties() {
    try (ClientX509Util util = new ClientX509Util()) {
      for (String key : new String[]{util.getSslContextSupplierClassProperty(), util.getSslProviderProperty(),
          util.getSslHostnameVerificationEnabledProperty(), util.getFipsModeProperty(),
          util.getSslCipherSuitesProperty(), util.getSslTruststoreLocationProperty(),
          util.getSslTruststorePasswdProperty(), util.getSslTruststoreTypeProperty()}) {
        _savedProperties.put(key, System.getProperty(key));
      }
      System.clearProperty(util.getSslContextSupplierClassProperty());
    }
  }

  @AfterMethod(alwaysRun = true)
  public void restoreBootstrapProperties() {
    _savedProperties.forEach((key, value) -> {
      if (value == null) {
        System.clearProperty(key);
      } else {
        System.setProperty(key, value);
      }
    });
    _savedProperties.clear();
  }

  @Test
  public void testCustomSupplierKeepsProviderOwnership() {
    try (ClientX509Util util = new ClientX509Util();
        MockedStatic<HelixManagerFactory> ignored = mockStatic(HelixManagerFactory.class)) {
      System.setProperty(util.getSslHostnameVerificationEnabledProperty(), "false");
      System.setProperty(util.getSslContextSupplierClassProperty(), "custom.supplier");
      System.setProperty(util.getSslProviderProperty(), "OPENSSL");
      System.setProperty(util.getSslCipherSuitesProperty(), "ECDHE-RSA-AES128-GCM-SHA256");
      HelixHelper.createZkHelixManager("test-cluster", "test-instance", InstanceType.SPECTATOR, "localhost:2181");
      assertEquals(System.getProperty(util.getSslContextSupplierClassProperty()), "custom.supplier");
      assertEquals(System.getProperty(util.getSslProviderProperty()), "OPENSSL");
      assertEquals(System.getProperty(util.getSslCipherSuitesProperty()), "ECDHE-RSA-AES128-GCM-SHA256");
    }
  }

  @DataProvider
  public Object[][] hostnamePolicies() {
    return new Object[][]{{false, false, "OPENSSL"}, {false, true, "OPENSSL_REFCNT"},
        {true, false, "JDK"}, {true, true, "JDK"}};
  }

  @Test(dataProvider = "hostnamePolicies")
  public void testConfiguredHostnamePolicy(boolean verifyHostname, boolean fips, String provider)
      throws Exception {
    try (ClientX509Util util = new ClientX509Util();
        MockedStatic<HelixManagerFactory> factory = mockStatic(HelixManagerFactory.class)) {
      System.setProperty(util.getSslHostnameVerificationEnabledProperty(), Boolean.toString(verifyHostname));
      System.setProperty(util.getFipsModeProperty(), Boolean.toString(fips));
      System.setProperty(util.getSslProviderProperty(), provider);
      System.setProperty(util.getSslCipherSuitesProperty(),
          verifyHostname ? "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" : "ECDHE-RSA-AES128-GCM-SHA256");
      System.setProperty(util.getSslTruststoreLocationProperty(), resourcePath("truststore.p12").toString());
      System.setProperty(util.getSslTruststorePasswdProperty(), "changeit");
      System.setProperty(util.getSslTruststoreTypeProperty(), "PKCS12");
      AtomicReference<SslContext> context = new AtomicReference<>();
      // Helix reads cloud config during manager construction. Capture that first client's context before returning.
      factory.when(() -> HelixManagerFactory.getZKHelixManager("test-cluster", "test-instance", InstanceType.SPECTATOR,
          "localhost:2181"))
          .thenAnswer(invocation -> {
            context.set(util.createNettySslContextForClient(new ZKConfig()));
            return null;
          });
      HelixHelper.createZkHelixManager("test-cluster", "test-instance", InstanceType.SPECTATOR, "localhost:2181");
      SslContext clientContext = context.get();

      KeyStore keys = KeyStore.getInstance("PKCS12");
      try (InputStream stream = Files.newInputStream(resourcePath("keystore.p12"))) {
        keys.load(stream, "changeit".toCharArray());
      }
      KeyManagerFactory keyManager = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManager.init(keys, "changeit".toCharArray());
      SSLContext serverContext = SSLContext.getInstance("TLS");
      serverContext.init(keyManager.getKeyManagers(), null, null);
      EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
      Channel channel = null;
      try (SSLServerSocket server = (SSLServerSocket) serverContext.getServerSocketFactory().createServerSocket()) {
        server.bind(new InetSocketAddress("127.0.0.1", 0));
        server.setSoTimeout(5000);
        int port = server.getLocalPort();
        SslHandler ssl = clientContext.newHandler(ByteBufAllocator.DEFAULT, "127.0.0.1", port);
        Thread worker = new Thread(() -> {
          try (SSLSocket socket = (SSLSocket) server.accept()) {
            socket.setSoTimeout(5000);
            socket.startHandshake();
          } catch (Exception e) {
            // An enabled-policy client rejects this deliberately mismatched certificate and closes its connection.
          }
        }, "zookeeper-tls-test-server");
        worker.setDaemon(true);
        worker.start();
        try {
          channel = new Bootstrap().group(group).channel(NioSocketChannel.class)
              .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socket) {
                  ssl.setHandshakeTimeoutMillis(5000);
                  socket.pipeline().addLast(ssl);
                }
              }).connect("127.0.0.1", port).sync().channel();
          assertTrue(ssl.handshakeFuture().await(6000), "TLS handshake did not complete");
          if (verifyHostname) {
            assertFalse(ssl.handshakeFuture().isSuccess());
            assertTrue(hasCertificateFailure(ssl.handshakeFuture().cause()),
                "Expected a certificate rejection, not a connection or timeout failure");
          } else {
            assertTrue(ssl.handshakeFuture().isSuccess(), String.valueOf(ssl.handshakeFuture().cause()));
            assertEquals(ssl.engine().getSession().getCipherSuite(),
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
          }
        } finally {
          if (channel != null) {
            channel.close().sync();
          }
          server.close();
          worker.join(6000);
          assertFalse(worker.isAlive(), "TLS fixture did not stop");
        }
      } finally {
        group.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
      }
    }
  }

  private static boolean hasCertificateFailure(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof CertificateException) {
        return true;
      }
    }
    return false;
  }

  private static Path resourcePath(String name)
      throws Exception {
    return Path.of(ZooKeeperSslContextSupplierTest.class.getClassLoader().getResource("tls/" + name).toURI());
  }
}
