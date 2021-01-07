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

import java.util.List;

import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.listeners.ListenerConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Asserts that {@link ListenerConfigUtil} will generated expected {@link ListenerConfig} based on the properties provided in {@link ControllerConf}
 */
public class ListenerConfigUtilTest {
  /**
   * Asserts that the protocol listeners properties are Opt-In and not initialized when nothing but controler.port is used.
   */
  @Test
  public void assertControllerPortConfig() {
    ControllerConf controllerConf = new ControllerConf();

    controllerConf.setProperty("controller.port", "9000");
    controllerConf.setProperty("controller.query.console.useHttps", "true");

    List<ListenerConfig> listenerConfigs = ListenerConfigUtil.buildListenerConfigs(controllerConf);

    Assert.assertEquals(listenerConfigs.size(), 1);

    assertLegacyListener(listenerConfigs.get(0));
  }

  /**
   * Asserts that enabling https generates the existing legacy listener as well as the another one configured with TLS settings. 
   */
  @Test
  public void assertLegacyAndHttps() {
    ControllerConf controllerConf = new ControllerConf();

    controllerConf.setProperty("controller.port", "9000");
    controllerConf.setProperty("controller.access.protocols", "https");

    configureHttpsProperties(controllerConf, "10.0.0.10", 9443);

    List<ListenerConfig> listenerConfigs = ListenerConfigUtil.buildListenerConfigs(controllerConf);

    Assert.assertEquals(listenerConfigs.size(), 2);

    ListenerConfig legacyListener = getListener("http", listenerConfigs);
    ListenerConfig httpsListener = getListener("https", listenerConfigs);

    assertLegacyListener(legacyListener);
    assertHttpsListener(httpsListener, "10.0.0.10", 9443);
  }

  /**
   * Asserts that controller.port can be opt-out and both http and https can be configured with seperate ports.
   */
  @Test
  public void assertHttpAndHttpsConfigs() {
    ControllerConf controllerConf = new ControllerConf();

    controllerConf.setProperty("controller.access.protocols", "http,https");

    configureHttpsProperties(controllerConf, 9443);

    controllerConf.setProperty("controller.access.protocols.http.port", "9000");

    List<ListenerConfig> listenerConfigs = ListenerConfigUtil.buildListenerConfigs(controllerConf);

    Assert.assertEquals(listenerConfigs.size(), 2);

    ListenerConfig httpListener = getListener("http", listenerConfigs);
    ListenerConfig httpsListener = getListener("https", listenerConfigs);

    Assert.assertEquals(httpListener.getHost(), "0.0.0.0");

    assertHttpListener(httpListener, "0.0.0.0", 9000);
    assertHttpsListener(httpsListener, "0.0.0.0", 9443);
  }

  /**
   * Asserts that a single listener configuration is generated with a secured TLS port.
   */
  @Test
  public void assertHttpsOnly() {
    ControllerConf controllerConf = new ControllerConf();

    controllerConf.setProperty("controller.access.protocols", "https");

    configureHttpsProperties(controllerConf, 9443);

    List<ListenerConfig> listenerConfigs = ListenerConfigUtil.buildListenerConfigs(controllerConf);

    Assert.assertEquals(listenerConfigs.size(), 1);

    assertHttpsListener(listenerConfigs.get(0), "0.0.0.0", 9443);
  }

  /**
   * Tests behavior when an invalid host is provided.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void assertInvalidHost() {
    ControllerConf controllerConf = new ControllerConf();

    controllerConf.setProperty("controller.access.protocols", "https");

    configureHttpsProperties(controllerConf, "", 9443);

    ListenerConfigUtil.buildListenerConfigs(controllerConf);
  }

  /**
   * Tests behavior when an invalid port is provided
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void assertInvalidPort() {
    ControllerConf controllerConf = new ControllerConf();

    controllerConf.setProperty("controller.access.protocols", "https");

    configureHttpsProperties(controllerConf, "", 9443);
    controllerConf.setProperty("controller.access.protocol.https.port", "10.10");

    ListenerConfigUtil.buildListenerConfigs(controllerConf);
  }

  /**
   * Tests behavior when an empty http port is provided.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void assertEmptyHttpPort() {
    ControllerConf controllerConf = new ControllerConf();

    controllerConf.setProperty("controller.access.protocols", "http");
    controllerConf.setProperty("controller.access.protocols.http.port", "");

    ListenerConfigUtil.buildListenerConfigs(controllerConf);
  }

  /**
   * Tests behavior when an empty https port is provided.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void assertEmptyHttpsPort() {
    ControllerConf controllerConf = new ControllerConf();

    controllerConf.setProperty("controller.access.protocols", "https");
    controllerConf.setProperty("controller.access.protocols.https.port", "");

    ListenerConfigUtil.buildListenerConfigs(controllerConf);
  }

  private void assertLegacyListener(ListenerConfig legacyListener) {
    Assert.assertEquals(legacyListener.getName(), "http");
    Assert.assertEquals(legacyListener.getHost(), "0.0.0.0");
    Assert.assertEquals(legacyListener.getPort(), 9000);
    Assert.assertEquals(legacyListener.getProtocol(), "http");
    Assert.assertFalse(legacyListener.getTlsConfig().isEnabled());
  }

  private void assertHttpListener(ListenerConfig httpsListener, String host, int port) {
    Assert.assertEquals(httpsListener.getName(), "http");
    Assert.assertEquals(httpsListener.getHost(), host);
    Assert.assertEquals(httpsListener.getPort(), port);
    Assert.assertEquals(httpsListener.getProtocol(), "http");
    Assert.assertFalse(httpsListener.getTlsConfig().isEnabled());
  }

  private void assertHttpsListener(ListenerConfig httpsListener, String host, int port) {
    Assert.assertEquals(httpsListener.getName(), "https");
    Assert.assertEquals(httpsListener.getHost(), host);
    Assert.assertEquals(httpsListener.getPort(), port);
    Assert.assertEquals(httpsListener.getProtocol(), "https");
    Assert.assertNotNull(httpsListener.getTlsConfig());
    Assert.assertTrue(httpsListener.getTlsConfig().isEnabled());
    Assert.assertEquals(httpsListener.getTlsConfig().getKeyStorePassword(), "a-password");
    Assert.assertEquals(httpsListener.getTlsConfig().getKeyStorePath(), "/some-keystore-path");
    Assert.assertTrue(httpsListener.getTlsConfig().isClientAuth());
    Assert.assertEquals(httpsListener.getTlsConfig().getTrustStorePassword(), "a-password");
    Assert.assertEquals(httpsListener.getTlsConfig().getTrustStorePath(), "/some-truststore-path");
  }

  private void configureHttpsProperties(ControllerConf controllerConf, String host, int port) {
    if (host != null) {
      controllerConf.setProperty("controller.access.protocols.https.host", host);
    }
    controllerConf.setProperty("controller.access.protocols.https.port", String.valueOf(port));
    controllerConf.setProperty("controller.access.protocols.https.tls.keystore.password", "a-password");
    controllerConf.setProperty("controller.access.protocols.https.tls.keystore.path", "/some-keystore-path");
    controllerConf.setProperty("controller.access.protocols.https.tls.client.auth", "true");
    controllerConf.setProperty("controller.access.protocols.https.tls.truststore.password", "a-password");
    controllerConf.setProperty("controller.access.protocols.https.tls.truststore.path", "/some-truststore-path");
  }

  private void configureHttpsProperties(ControllerConf controllerConf, int port) {
    configureHttpsProperties(controllerConf, null, port);
  }

  private ListenerConfig getListener(String name, List<ListenerConfig> listenerConfigs) {
    return listenerConfigs.stream().filter(listenerConfig -> listenerConfig.getName().equals(name)).findFirst().get();
  }
}
