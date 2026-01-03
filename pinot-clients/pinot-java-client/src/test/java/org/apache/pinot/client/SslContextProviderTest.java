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

import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.SslEngineFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class SslContextProviderTest {
  private static final String PROVIDER_PROPERTY = "pinot.client.sslContextProvider";

  @Test
  public void testDefaultProviderConfiguresProtocolsAndDisablesOpenSsl()
      throws Exception {
    DefaultAsyncHttpClientConfig.Builder builder = Dsl.config();
    SSLContext probeContext = SSLContext.getInstance("TLS");
    probeContext.init(null, null, null);
    String protocol = selectSupportedProtocol(probeContext);
    TlsProtocols tlsProtocols = tlsProtocolsWith(protocol);

    SslContextProvider provider = new DefaultSslContextProvider();
    provider.configure(builder, null, tlsProtocols);

    DefaultAsyncHttpClientConfig config = builder.build();
    assertFalse(config.isUseOpenSsl());
    assertEquals(config.getEnabledProtocols(), new String[] {protocol});
  }

  @Test
  public void testDefaultProviderCreatesClientSslEngine()
      throws Exception {
    DefaultAsyncHttpClientConfig.Builder builder = Dsl.config();

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, null, null);
    String protocol = selectSupportedProtocol(sslContext);

    SslContextProvider provider = new DefaultSslContextProvider();
    provider.configure(builder, sslContext, tlsProtocolsWith(protocol));

    DefaultAsyncHttpClientConfig config = builder.build();
    assertFalse(config.isUseOpenSsl());
    assertEquals(config.getEnabledProtocols(), new String[] {protocol});

    SslEngineFactory engineFactory = config.getSslEngineFactory();
    assertNotNull(engineFactory);
    SSLEngine engine = engineFactory.newSslEngine(config, "localhost", 443);
    assertTrue(engine.getUseClientMode());
    assertEquals(engine.getEnabledProtocols(), new String[] {protocol});
  }

  @Test
  public void testFactoryUsesSystemPropertyProvider() {
    String originalProperty = System.getProperty(PROVIDER_PROPERTY);
    System.setProperty(PROVIDER_PROPERTY, PropertyProvider.class.getName());
    try {
      SslContextProvider provider = SslContextProviderFactory.create();
      assertTrue(provider instanceof PropertyProvider);
    } finally {
      restoreProperty(originalProperty);
    }
  }

  @Test
  public void testFactoryUsesServiceLoaderProviderWhenPropertyMissing()
      throws Exception {
    String originalProperty = System.getProperty(PROVIDER_PROPERTY);
    restoreProperty(null);
    try {
      SslContextProvider provider = createFromServiceLoader(ServiceLoaderProvider.class);
      assertTrue(provider instanceof ServiceLoaderProvider);
    } finally {
      restoreProperty(originalProperty);
    }
  }

  @Test
  public void testFactoryAcceptsDefaultProviderSubclassFromServiceLoader()
      throws Exception {
    String originalProperty = System.getProperty(PROVIDER_PROPERTY);
    restoreProperty(null);
    try {
      SslContextProvider provider = createFromServiceLoader(DefaultProviderSubclass.class);
      assertTrue(provider instanceof DefaultProviderSubclass);
    } finally {
      restoreProperty(originalProperty);
    }
  }

  @Test
  public void testFactoryFallsBackToDefaultWhenPropertyInvalidAndNoServiceLoader() {
    String originalProperty = System.getProperty(PROVIDER_PROPERTY);
    System.setProperty(PROVIDER_PROPERTY, String.class.getName());
    try {
      SslContextProvider provider = SslContextProviderFactory.create();
      assertTrue(provider instanceof DefaultSslContextProvider);
    } finally {
      restoreProperty(originalProperty);
    }
  }

  private static void restoreProperty(@Nullable String value) {
    if (value == null) {
      System.clearProperty(PROVIDER_PROPERTY);
    } else {
      System.setProperty(PROVIDER_PROPERTY, value);
    }
  }

  private static SslContextProvider createFromServiceLoader(Class<? extends SslContextProvider> providerClass)
      throws Exception {
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();

    Path tempDir = Files.createTempDirectory("pinot-ssl-provider");
    Path servicesDir = tempDir.resolve("META-INF").resolve("services");
    Files.createDirectories(servicesDir);
    Path serviceFile = servicesDir.resolve(SslContextProvider.class.getName());
    Files.writeString(serviceFile, providerClass.getName(), StandardCharsets.UTF_8);

    try (URLClassLoader loader =
        new URLClassLoader(new URL[] {tempDir.toUri().toURL()}, SslContextProviderTest.class.getClassLoader())) {
      Thread.currentThread().setContextClassLoader(loader);
      return SslContextProviderFactory.create();
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  private static TlsProtocols tlsProtocolsWith(String... protocols)
      throws Exception {
    Constructor<TlsProtocols> constructor = TlsProtocols.class.getDeclaredConstructor(List.class);
    constructor.setAccessible(true);
    return constructor.newInstance(Arrays.asList(protocols));
  }

  private static String selectSupportedProtocol(SSLContext sslContext) {
    List<String> supported = Arrays.asList(sslContext.getSupportedSSLParameters().getProtocols());
    if (supported.contains("TLSv1.2")) {
      return "TLSv1.2";
    }
    return supported.get(0);
  }

  public static class PropertyProvider implements SslContextProvider {
    @Override
    public DefaultAsyncHttpClientConfig.Builder configure(DefaultAsyncHttpClientConfig.Builder builder,
        @Nullable SSLContext sslContext, TlsProtocols tlsProtocols) {
      return builder;
    }
  }

  public static class ServiceLoaderProvider implements SslContextProvider {
    @Override
    public DefaultAsyncHttpClientConfig.Builder configure(DefaultAsyncHttpClientConfig.Builder builder,
        @Nullable SSLContext sslContext, TlsProtocols tlsProtocols) {
      return builder;
    }
  }

  public static class DefaultProviderSubclass extends DefaultSslContextProvider {
  }
}
