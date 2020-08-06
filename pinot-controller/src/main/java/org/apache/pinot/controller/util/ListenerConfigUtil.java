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
package org.apache.pinot.controller.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.listeners.ListenerConfig;
import org.apache.pinot.controller.api.listeners.TlsConfiguration;


/**
 * Utility class that generates Http {@link ListenerConfig} instances 
 * based on the properties provided by {@link ControllerConf}.
 */
public abstract class ListenerConfigUtil {

  /**
   * Generates {@link ListenerConfig} instances based on the compination 
   * of propperties such as controller.port and controller.access.protocols.
   * 
   * @param controllerConf property holders for controller configuration
   * @return List of {@link ListenerConfig} for which http listeners 
   * should be created.
   */
  public static List<ListenerConfig> buildListenerConfigs(ControllerConf controllerConf) {
    List<ListenerConfig> listenerConfigs = new ArrayList<>();

    if (controllerConf.getControllerPort() != null) {
      listenerConfigs.add(
          new ListenerConfig("http", "0.0.0.0", Integer.valueOf(controllerConf.getControllerPort()), "http", null));
    }

    listenerConfigs.addAll(controllerConf.getControllerAccessProtocols().stream()

        .map(protocol -> buildListenerConfig(protocol, controllerConf))

        .collect(Collectors.toList()));

    return listenerConfigs;
  }

  private static Optional<TlsConfiguration> buildTlsConfiguration(String protocol, ControllerConf controllerConf) {
    return Optional.ofNullable(controllerConf.getControllerAccessProtocolProperty(protocol, "tls.keystore.path"))

        .map(keystore -> buildTlsConfiguration(protocol, keystore, controllerConf));
  }

  private static TlsConfiguration buildTlsConfiguration(String protocol, String keystore,
      ControllerConf controllerConf) {
    return new TlsConfiguration(keystore,
        controllerConf.getControllerAccessProtocolProperty(protocol, "tls.keystore.password"),
        controllerConf.getControllerAccessProtocolProperty(protocol, "tls.truststore.path"),
        controllerConf.getControllerAccessProtocolProperty(protocol, "tls.truststore.password"), Boolean.parseBoolean(
            controllerConf.getControllerAccessProtocolProperty(protocol, "tls.requires_client_auth", "false")));
  }

  private static ListenerConfig buildListenerConfig(String protocol, ControllerConf controllerConf) {
    return new ListenerConfig(protocol,
        getHost(controllerConf.getControllerAccessProtocolProperty(protocol, "host", "0.0.0.0")),
        getPort(controllerConf.getControllerAccessProtocolProperty(protocol, "port")), protocol,
        buildTlsConfiguration(protocol, controllerConf).orElse(null));
  }

  private static String getHost(String configuredHost) {
    return Optional.ofNullable(configuredHost).filter(host -> !host.trim().isEmpty())
        .orElseThrow(() -> new IllegalArgumentException(configuredHost + " is not a valid host"));
  }

  private static int getPort(String configuredPort) {
    return Optional.ofNullable(configuredPort).filter(host -> !host.trim().isEmpty()).<Integer> map(Integer::valueOf)
        .orElseThrow(() -> new IllegalArgumentException(configuredPort + " is not a valid port"));
  }
}
