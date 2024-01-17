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
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.tls.TlsUtils;
import org.apache.pinot.core.transport.HttpServerThreadPoolConfig;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.ssl.SSLContextConfigurator;
import org.glassfish.grizzly.ssl.SSLEngineConfigurator;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.internal.guava.ThreadFactoryBuilder;
import org.glassfish.jersey.process.JerseyProcessingUncaughtExceptionHandler;
import org.glassfish.jersey.server.ResourceConfig;

import static org.apache.pinot.spi.utils.CommonConstants.HTTPS_PROTOCOL;


/**
 * Utility class that generates Http {@link ListenerConfig} instances
 * based on the properties provided by a property namespace in {@link PinotConfiguration}.
 */
public final class ListenerConfigUtil {
  private static final String DEFAULT_HOST = "0.0.0.0";
  private static final String DOT_ACCESS_PROTOCOLS = ".access.protocols";
  private static final String DOT_ACCESS_THREAD_POOL = ".http.server.thread.pool";

  private ListenerConfigUtil() {
    // left blank
  }

  public static final Set<String> SUPPORTED_PROTOCOLS =
      new HashSet<>(Arrays.asList(CommonConstants.HTTP_PROTOCOL, CommonConstants.HTTPS_PROTOCOL));

  /**
   * Generates {@link ListenerConfig} instances based on the combination
   * of properties such as *.port and *.access.protocols.
   *
   * @param config property holders for controller configuration
   * @param namespace property namespace to extract from
   *
   * @return List of {@link ListenerConfig} for which http listeners
   * should be created.
   */
  public static List<ListenerConfig> buildListenerConfigs(PinotConfiguration config, String namespace,
      TlsConfig tlsDefaults) {
    if (StringUtils.isBlank(config.getProperty(namespace + DOT_ACCESS_PROTOCOLS))) {
      return new ArrayList<>();
    }

    String[] protocols = config.getProperty(namespace + DOT_ACCESS_PROTOCOLS).split(",");

    return Arrays.stream(protocols).map(protocol -> buildListenerConfig(config, namespace, protocol, tlsDefaults))
        .collect(Collectors.toList());
  }

  public static List<ListenerConfig> buildControllerConfigs(PinotConfiguration controllerConf) {
    List<ListenerConfig> listeners = new ArrayList<>();

    String portString = controllerConf.getProperty("controller.port");
    if (portString != null) {
      listeners.add(new ListenerConfig(CommonConstants.HTTP_PROTOCOL, DEFAULT_HOST, Integer.parseInt(portString),
          CommonConstants.HTTP_PROTOCOL, new TlsConfig(), buildServerThreadPoolConfig(controllerConf,
          "pinot.controller")));
    }

    TlsConfig tlsDefaults = TlsUtils.extractTlsConfig(controllerConf, "controller.tls");

    listeners.addAll(buildListenerConfigs(controllerConf, "controller", tlsDefaults));

    Preconditions.checkState(!listeners.isEmpty(), "Missing listener configs");
    return listeners;
  }

  public static List<ListenerConfig> buildBrokerConfigs(PinotConfiguration brokerConf) {
    List<ListenerConfig> listeners = new ArrayList<>();

    String queryPortString = brokerConf.getProperty(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT);
    if (queryPortString != null) {
      listeners.add(new ListenerConfig(CommonConstants.HTTP_PROTOCOL, DEFAULT_HOST, Integer.parseInt(queryPortString),
          CommonConstants.HTTP_PROTOCOL, new TlsConfig(), buildServerThreadPoolConfig(brokerConf, "pinot.broker")));
    }

    TlsConfig tlsDefaults = TlsUtils.extractTlsConfig(brokerConf, CommonConstants.Broker.BROKER_TLS_PREFIX);

    listeners.addAll(buildListenerConfigs(brokerConf, "pinot.broker.client", tlsDefaults));

    // support legacy behavior < 0.7.0
    if (listeners.isEmpty()) {
      listeners.add(new ListenerConfig(CommonConstants.HTTP_PROTOCOL, DEFAULT_HOST,
          CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT, CommonConstants.HTTP_PROTOCOL, new TlsConfig(),
          buildServerThreadPoolConfig(brokerConf, "pinot.broker")));
    }

    return listeners;
  }

  public static List<ListenerConfig> buildServerAdminConfigs(PinotConfiguration serverConf) {
    List<ListenerConfig> listeners = new ArrayList<>();

    String adminApiPortString = serverConf.getProperty(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT);
    if (adminApiPortString != null) {
      listeners.add(
          new ListenerConfig(CommonConstants.HTTP_PROTOCOL, DEFAULT_HOST, Integer.parseInt(adminApiPortString),
              CommonConstants.HTTP_PROTOCOL, new TlsConfig(), buildServerThreadPoolConfig(serverConf, "pinot.server")));
    }

    TlsConfig tlsDefaults = TlsUtils.extractTlsConfig(serverConf, CommonConstants.Server.SERVER_TLS_PREFIX);

    listeners.addAll(buildListenerConfigs(serverConf, "pinot.server.adminapi", tlsDefaults));

    // support legacy behavior < 0.7.0
    if (listeners.isEmpty()) {
      listeners.add(
          new ListenerConfig(CommonConstants.HTTP_PROTOCOL, DEFAULT_HOST, CommonConstants.Server.DEFAULT_ADMIN_API_PORT,
              CommonConstants.HTTP_PROTOCOL, new TlsConfig(), buildServerThreadPoolConfig(serverConf, "pinot.server")));
    }

    return listeners;
  }

  public static List<ListenerConfig> buildMinionConfigs(PinotConfiguration minionConf) {
    List<ListenerConfig> listeners = new ArrayList<>();

    String portString = minionConf.getProperty(CommonConstants.Helix.KEY_OF_MINION_PORT);
    if (portString != null) {
      listeners.add(new ListenerConfig(CommonConstants.HTTP_PROTOCOL, DEFAULT_HOST, Integer.parseInt(portString),
          CommonConstants.HTTP_PROTOCOL, new TlsConfig(), buildServerThreadPoolConfig(minionConf, "pinot.minion")));
    }

    TlsConfig tlsDefaults = TlsUtils.extractTlsConfig(minionConf, CommonConstants.Minion.MINION_TLS_PREFIX);
    listeners.addAll(buildListenerConfigs(minionConf, "pinot.minion.adminapi", tlsDefaults));

    // support legacy behavior < 0.7.0
    if (listeners.isEmpty()) {
      listeners.add(
          new ListenerConfig(CommonConstants.HTTP_PROTOCOL, DEFAULT_HOST, CommonConstants.Minion.DEFAULT_HELIX_PORT,
              CommonConstants.HTTP_PROTOCOL, new TlsConfig(), buildServerThreadPoolConfig(minionConf, "pinot.minion")));
    }

    return listeners;
  }

  private static ListenerConfig buildListenerConfig(PinotConfiguration config, String namespace, String name,
      TlsConfig tlsConfig) {
    String protocolNamespace = namespace + DOT_ACCESS_PROTOCOLS + "." + name;

    return new ListenerConfig(name, getHost(config.getProperty(protocolNamespace + ".host", DEFAULT_HOST)),
        getPort(config.getProperty(protocolNamespace + ".port")),
        getProtocol(config.getProperty(protocolNamespace + ".protocol"), name),
        TlsUtils.extractTlsConfig(config, protocolNamespace + ".tls", tlsConfig),
        buildServerThreadPoolConfig(config, namespace));
  }

  private static String getHost(String configuredHost) {
    return Optional.ofNullable(configuredHost).map(String::trim).filter(host -> !host.isEmpty())
        .orElseThrow(() -> new IllegalArgumentException(configuredHost + " is not a valid host"));
  }

  private static int getPort(String configuredPort) {
    return Optional.ofNullable(configuredPort).map(String::trim).filter(port -> !port.isEmpty()).map(Integer::valueOf)
        .orElseThrow(() -> new IllegalArgumentException(configuredPort + " is not a valid port"));
  }

  private static String getProtocol(String configuredProtocol, String listenerName) {
    Optional<String> optProtocol =
        Optional.ofNullable(configuredProtocol).map(String::trim).filter(protocol -> !protocol.isEmpty());
    if (!optProtocol.isPresent()) {
      return Optional.of(listenerName).filter(SUPPORTED_PROTOCOLS::contains).orElseThrow(
          () -> new IllegalArgumentException("No protocol set for listener" + listenerName + " and '" + listenerName
              + "' is not a valid protocol either"));
    }
    return optProtocol.filter(SUPPORTED_PROTOCOLS::contains)
        .orElseThrow(() -> new IllegalArgumentException(configuredProtocol + " is not a valid protocol"));
  }

  public static HttpServer buildHttpServer(ResourceConfig resConfig, List<ListenerConfig> listenerConfigs) {
    Preconditions.checkNotNull(listenerConfigs);

    // The URI is irrelevant since the default listener will be manually rewritten.
    HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(URI.create("http://0.0.0.0/"), resConfig, false);

    // Listeners cannot be configured with the factory. Manual overrides are required as instructed by Javadoc.
    // @see org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory.createHttpServer(java.net.URI, org
    // .glassfish.jersey.grizzly2.httpserver.GrizzlyHttpContainer, boolean, org.glassfish.grizzly.ssl
    // .SSLEngineConfigurator, boolean)
    httpServer.removeListener("grizzly");

    listenerConfigs.forEach(listenerConfig -> configureListener(httpServer, listenerConfig));

    return httpServer;
  }

  public static void configureListener(HttpServer httpServer, ListenerConfig listenerConfig) {
    final NetworkListener listener =
        new NetworkListener(listenerConfig.getName() + "-" + listenerConfig.getPort(), listenerConfig.getHost(),
            listenerConfig.getPort());

    listener.getTransport().getWorkerThreadPoolConfig().setThreadFactory(
        new ThreadFactoryBuilder().setNameFormat("grizzly-http-server-%d")
            .setUncaughtExceptionHandler(new JerseyProcessingUncaughtExceptionHandler()).build())
        .setCorePoolSize(listenerConfig.getThreadPoolConfig().getCorePoolSize())
        .setMaxPoolSize(listenerConfig.getThreadPoolConfig().getMaxPoolSize());

    if (CommonConstants.HTTPS_PROTOCOL.equals(listenerConfig.getProtocol())) {
      listener.setSecure(true);
      listener.setSSLEngineConfig(buildSSLEngineConfigurator(listenerConfig.getTlsConfig()));
    }

    httpServer.addListener(listener);
  }

  /**
   * Finds the last listener that has HTTPS protocol, and returns its port. If not found any TLS, return defaultValue
   * @param configs the config to search
   * @param defaultValue the default value if the TLS listener is not found
   * @return the port number of last entry that has secure protocol. If not found then defaultValue
   */
  public static int findLastTlsPort(List<ListenerConfig> configs, int defaultValue) {
    return configs.stream()
        .filter(config -> config.getProtocol().equalsIgnoreCase(HTTPS_PROTOCOL))
        .map(ListenerConfig::getPort)
        .reduce((first, second) -> second)
        .orElse(defaultValue);
  }

  private static SSLEngineConfigurator buildSSLEngineConfigurator(TlsConfig tlsConfig) {
    SSLContextConfigurator sslContextConfigurator = new SSLContextConfigurator();

    if (tlsConfig.getKeyStorePath() != null) {
      Preconditions.checkNotNull(tlsConfig.getKeyStorePassword(), "key store password required");
      sslContextConfigurator.setKeyStoreFile(cacheInTempFile(tlsConfig.getKeyStorePath()).getAbsolutePath());
      sslContextConfigurator.setKeyStorePass(tlsConfig.getKeyStorePassword());
    }

    if (tlsConfig.getTrustStorePath() != null) {
      Preconditions.checkNotNull(tlsConfig.getKeyStorePassword(), "trust store password required");
      sslContextConfigurator.setTrustStoreFile(cacheInTempFile(tlsConfig.getTrustStorePath()).getAbsolutePath());
      sslContextConfigurator.setTrustStorePass(tlsConfig.getTrustStorePassword());
    }

    return new SSLEngineConfigurator(sslContextConfigurator).setClientMode(false)
        .setNeedClientAuth(tlsConfig.isClientAuthEnabled()).setEnabledProtocols(new String[]{"TLSv1.2"});
  }

  private static HttpServerThreadPoolConfig buildServerThreadPoolConfig(PinotConfiguration config, String namespace) {
    String threadPoolNamespace = namespace + DOT_ACCESS_THREAD_POOL;

    HttpServerThreadPoolConfig threadPoolConfig = HttpServerThreadPoolConfig.defaultInstance();
    int corePoolSize = config.getProperty(threadPoolNamespace + "." + "corePoolSize", -1);
    int maxPoolSize = config.getProperty(threadPoolNamespace + "." + "maxPoolSize", -1);
    if (corePoolSize > 0) {
      threadPoolConfig.setCorePoolSize(corePoolSize);
    }
    if (maxPoolSize > 0) {
      threadPoolConfig.setMaxPoolSize(maxPoolSize);
    }
    return threadPoolConfig;
  }

  public static String toString(Collection<? extends ListenerConfig> listenerConfigs) {
    return StringUtils.join(listenerConfigs.stream()
        .map(listener -> String.format("%s://%s:%d", listener.getProtocol(), listener.getHost(), listener.getPort()))
        .toArray(), ", ");
  }

  private static File cacheInTempFile(String sourceUrl) {
    try {
      URL url = TlsUtils.makeKeyStoreUrl(sourceUrl);
      if ("file".equals(url.getProtocol())) {
        return new File(url.getPath());
      }

      File tempFile = Files.createTempFile("pinot-keystore-", null).toFile();
      tempFile.deleteOnExit();

      try (InputStream is = url.openStream();
          OutputStream os = new FileOutputStream(tempFile)) {
        IOUtils.copy(is, os);
      }

      return tempFile;
    } catch (Exception e) {
      throw new IllegalStateException(String.format("Could not retrieve and cache keystore from '%s'", sourceUrl), e);
    }
  }
}
