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
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.transport.TlsConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.ssl.SSLContextConfigurator;
import org.glassfish.grizzly.ssl.SSLEngineConfigurator;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.internal.guava.ThreadFactoryBuilder;
import org.glassfish.jersey.process.JerseyProcessingUncaughtExceptionHandler;
import org.glassfish.jersey.server.ResourceConfig;


/**
 * Utility class that generates Http {@link ListenerConfig} instances 
 * based on the properties provided by a property namespace in {@link PinotConfiguration}.
 */
public final class ListenerConfigUtil {
  private static final String DEFAULT_HOST = "0.0.0.0";
  private static final String DOT_ACCESS_PROTOCOLS = ".access.protocols";

  private ListenerConfigUtil() {
    // left blank
  }

  public static final Set<String> SUPPORTED_PROTOCOLS = new HashSet<>(
      Arrays.asList(CommonConstants.HTTP_PROTOCOL, CommonConstants.HTTPS_PROTOCOL));

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

    return Arrays.stream(protocols)
        .peek(protocol -> Preconditions.checkArgument(SUPPORTED_PROTOCOLS.contains(protocol),
            "Unsupported protocol '%s' in config namespace '%s'", protocol, namespace))
        .map(protocol -> buildListenerConfig(config, namespace, protocol, tlsDefaults))
        .collect(Collectors.toList());
  }

  public static List<ListenerConfig> buildControllerConfigs(PinotConfiguration controllerConf) {
    List<ListenerConfig> listeners = new ArrayList<>();

    String portString = controllerConf.getProperty("controller.port");
    if (portString != null) {
      listeners.add(new ListenerConfig(CommonConstants.HTTP_PROTOCOL, DEFAULT_HOST, Integer.parseInt(portString),
          CommonConstants.HTTP_PROTOCOL, new TlsConfig()));
    }

    TlsConfig tlsDefaults = TlsUtils.extractTlsConfig(controllerConf, "controller.tls");

    listeners.addAll(buildListenerConfigs(controllerConf, "controller", tlsDefaults));

    return listeners;
  }

  public static List<ListenerConfig> buildBrokerConfigs(PinotConfiguration brokerConf) {
    List<ListenerConfig> listeners = new ArrayList<>();

    String queryPortString = brokerConf.getProperty(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT);
    if (queryPortString != null) {
      listeners.add(new ListenerConfig(CommonConstants.HTTP_PROTOCOL, DEFAULT_HOST, Integer.parseInt(queryPortString),
          CommonConstants.HTTP_PROTOCOL, new TlsConfig()));
    }

    TlsConfig tlsDefaults = TlsUtils.extractTlsConfig(brokerConf, CommonConstants.Broker.BROKER_TLS_PREFIX);

    listeners.addAll(buildListenerConfigs(brokerConf, "pinot.broker.client", tlsDefaults));

    // support legacy behavior < 0.7.0
    if (listeners.isEmpty()) {
      listeners.add(new ListenerConfig(CommonConstants.HTTP_PROTOCOL, DEFAULT_HOST,
          CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT, CommonConstants.HTTP_PROTOCOL, new TlsConfig()));
    }

    return listeners;
  }

  public static List<ListenerConfig> buildServerAdminConfigs(PinotConfiguration serverConf) {
    List<ListenerConfig> listeners = new ArrayList<>();

    String adminApiPortString = serverConf.getProperty(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT);
    if (adminApiPortString != null) {
      listeners.add(new ListenerConfig(CommonConstants.HTTP_PROTOCOL, DEFAULT_HOST, Integer.parseInt(adminApiPortString),
          CommonConstants.HTTP_PROTOCOL, new TlsConfig()));
    }

    TlsConfig tlsDefaults = TlsUtils.extractTlsConfig(serverConf, CommonConstants.Server.SERVER_TLS_PREFIX);

    listeners.addAll(buildListenerConfigs(serverConf, "pinot.server.adminapi", tlsDefaults));

    // support legacy behavior < 0.7.0
    if (listeners.isEmpty()) {
      listeners.add(new ListenerConfig(CommonConstants.HTTP_PROTOCOL, DEFAULT_HOST,
          CommonConstants.Server.DEFAULT_ADMIN_API_PORT, CommonConstants.HTTP_PROTOCOL, new TlsConfig()));
    }

    return listeners;
  }

  private static ListenerConfig buildListenerConfig(PinotConfiguration config, String namespace, String protocol,
      TlsConfig tlsConfig) {
    String protocolNamespace = namespace + DOT_ACCESS_PROTOCOLS + "." + protocol;

    return new ListenerConfig(protocol,
        getHost(config.getProperty(protocolNamespace + ".host", DEFAULT_HOST)),
        getPort(config.getProperty(protocolNamespace + ".port")), protocol, tlsConfig);
  }

  private static String getHost(String configuredHost) {
    return Optional.ofNullable(configuredHost).filter(host -> !host.trim().isEmpty())
        .orElseThrow(() -> new IllegalArgumentException(configuredHost + " is not a valid host"));
  }

  private static int getPort(String configuredPort) {
    return Optional.ofNullable(configuredPort).filter(port -> !port.trim().isEmpty()).<Integer> map(Integer::valueOf)
        .orElseThrow(() -> new IllegalArgumentException(configuredPort + " is not a valid port"));
  }

  public static HttpServer buildHttpServer(ResourceConfig resConfig, List<ListenerConfig> listenerConfigs) {
    Preconditions.checkNotNull(listenerConfigs);

    // The URI is irrelevant since the default listener will be manually rewritten.
    HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(URI.create("http://0.0.0.0/"), resConfig, false);

    // Listeners cannot be configured with the factory. Manual overrides are required as instructed by Javadoc.
    // @see org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory.createHttpServer(java.net.URI, org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpContainer, boolean, org.glassfish.grizzly.ssl.SSLEngineConfigurator, boolean)
    httpServer.removeListener("grizzly");

    listenerConfigs.forEach(listenerConfig -> configureListener(httpServer, listenerConfig));

    return httpServer;
  }

  public static void configureListener(HttpServer httpServer, ListenerConfig listenerConfig) {
    final NetworkListener listener = new NetworkListener(listenerConfig.getName() + "-" + listenerConfig.getPort(),
        listenerConfig.getHost(), listenerConfig.getPort());

    listener.getTransport().getWorkerThreadPoolConfig()
        .setThreadFactory(new ThreadFactoryBuilder().setNameFormat("grizzly-http-server-%d")
            .setUncaughtExceptionHandler(new JerseyProcessingUncaughtExceptionHandler()).build());

    if (CommonConstants.HTTPS_PROTOCOL.equals(listenerConfig.getProtocol())) {
      listener.setSecure(true);
      listener.setSSLEngineConfig(buildSSLConfig(listenerConfig.getTlsConfig()));
    }

    httpServer.addListener(listener);
  }

  private static SSLEngineConfigurator buildSSLConfig(TlsConfig tlsConfig) {
    SSLContextConfigurator sslContextConfigurator = new SSLContextConfigurator();

    if (tlsConfig.getKeyStorePath() != null) {
      Preconditions.checkNotNull(tlsConfig.getKeyStorePassword(), "key store password required");
      sslContextConfigurator.setKeyStoreFile(tlsConfig.getKeyStorePath());
      sslContextConfigurator.setKeyStorePass(tlsConfig.getKeyStorePassword());
    }
    if (tlsConfig.getTrustStorePath() != null) {
      Preconditions.checkNotNull(tlsConfig.getKeyStorePassword(), "trust store password required");
      sslContextConfigurator.setTrustStoreFile(tlsConfig.getTrustStorePath());
      sslContextConfigurator.setTrustStorePass(tlsConfig.getTrustStorePassword());
    }

    return new SSLEngineConfigurator(sslContextConfigurator).setClientMode(false)
        .setNeedClientAuth(tlsConfig.isClientAuthEnabled()).setEnabledProtocols(new String[] { "TLSv1.2" });
  }

  public static String toString(Collection<? extends ListenerConfig> listenerConfigs) {
    return StringUtils.join(listenerConfigs.stream()
        .map(listener -> String.format("%s://%s:%d", listener.getProtocol(), listener.getHost(), listener.getPort()))
        .toArray(), ", ");
  }
}
