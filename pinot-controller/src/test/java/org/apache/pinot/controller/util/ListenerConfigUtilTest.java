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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Asserts that {@link ListenerConfigUtil} will generated expected {@link ListenerConfig} based on the properties
 * provided in {@link ControllerConf}
 */
public class ListenerConfigUtilTest {
  /**
   * Asserts that the protocol listeners properties are Opt-In and not initialized when nothing but controler.port is
   * used.
   */
  @Test(expectedExceptions = IllegalStateException.class)
  public void testControllerPortConfig() {
    ControllerConf controllerConf = new ControllerConf();

    controllerConf.setProperty("controller.port", "9000");
    controllerConf.setProperty("controller.query.console.useHttps", "true");

    List<ListenerConfig> listenerConfigs = ListenerConfigUtil.buildControllerConfigs(controllerConf);
    Assert.assertEquals(listenerConfigs.size(), 1);
    assertLegacyListener(listenerConfigs.get(0));

    ListenerConfigUtil.buildControllerConfigs(new ControllerConf());
  }

  /**
   * Asserts that enabling https generates the existing legacy listener as well as the another one configured with
   * TLS settings.
   */
  @Test
  public void testLegacyAndHttps() {
    ControllerConf controllerConf = new ControllerConf();

    controllerConf.setProperty("controller.port", "9000");
    controllerConf.setProperty("controller.access.protocols", "https");

    configureHttpsProperties(controllerConf, "10.0.0.10", 9443);

    List<ListenerConfig> listenerConfigs = ListenerConfigUtil.buildControllerConfigs(controllerConf);

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
  public void testHttpAndHttpsConfigs() {
    ControllerConf controllerConf = new ControllerConf();

    controllerConf.setProperty("controller.access.protocols", "http,https");

    configureHttpsProperties(controllerConf, 9443);

    controllerConf.setProperty("controller.access.protocols.http.port", "9000");

    List<ListenerConfig> listenerConfigs = ListenerConfigUtil.buildControllerConfigs(controllerConf);

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
  public void testHttpsOnly() {
    ControllerConf controllerConf = new ControllerConf();

    controllerConf.setProperty("controller.access.protocols", "https");

    configureHttpsProperties(controllerConf, 9443);

    List<ListenerConfig> listenerConfigs = ListenerConfigUtil.buildControllerConfigs(controllerConf);

    Assert.assertEquals(listenerConfigs.size(), 1);

    assertHttpsListener(listenerConfigs.get(0), "0.0.0.0", 9443);
  }

  /**
   * Tests behavior when an invalid host is provided.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidHost() {
    ControllerConf controllerConf = new ControllerConf();

    controllerConf.setProperty("controller.access.protocols", "https");

    configureHttpsProperties(controllerConf, "", 9443);

    ListenerConfigUtil.buildControllerConfigs(controllerConf);
  }

  /**
   * Tests behavior when an invalid port is provided
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidPort() {
    ControllerConf controllerConf = new ControllerConf();

    controllerConf.setProperty("controller.access.protocols", "https");

    configureHttpsProperties(controllerConf, "", 9443);
    controllerConf.setProperty("controller.access.protocol.https.port", "10.10");

    ListenerConfigUtil.buildControllerConfigs(controllerConf);
  }

  /**
   * Tests behavior when an empty http port is provided.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEmptyHttpPort() {
    ControllerConf controllerConf = new ControllerConf();

    controllerConf.setProperty("controller.access.protocols", "http");
    controllerConf.setProperty("controller.access.protocols.http.port", "");

    ListenerConfigUtil.buildControllerConfigs(controllerConf);
  }

  /**
   * Tests behavior when an empty https port is provided.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEmptyHttpsPort() {
    ControllerConf controllerConf = new ControllerConf();

    controllerConf.setProperty("controller.access.protocols", "https");
    controllerConf.setProperty("controller.access.protocols.https.port", "");

    ListenerConfigUtil.buildControllerConfigs(controllerConf);
  }

  @Test
  public void testFindLastTlsPort() {
    List<ListenerConfig> configs = ImmutableList.of(new ListenerConfig("conf1", "host1", 9000, "http", null),
        new ListenerConfig("conf2", "host2", 9001, "https", null),
        new ListenerConfig("conf3", "host3", 9002, "http", null),
        new ListenerConfig("conf4", "host4", 9003, "https", null),
        new ListenerConfig("conf5", "host5", 9004, "http", null));
    int tlsPort = ListenerConfigUtil.findLastTlsPort(configs, -1);
    Assert.assertEquals(tlsPort, 9003);
  }

  @Test
  public void testFindLastTlsPortMissing() {
    List<ListenerConfig> configs = ImmutableList.of(new ListenerConfig("conf1", "host1", 9000, "http", null),
        new ListenerConfig("conf2", "host2", 9001, "http", null),
        new ListenerConfig("conf3", "host3", 9002, "http", null),
        new ListenerConfig("conf4", "host4", 9004, "http", null));
    int tlsPort = ListenerConfigUtil.findLastTlsPort(configs, -1);
    Assert.assertEquals(tlsPort, -1);
  }

  @Test
  public void testBuildMinionConfigs() {
    PinotConfiguration conf = new PinotConfiguration();
    List<ListenerConfig> listenerConfigs = ListenerConfigUtil.buildMinionConfigs(conf);
    Assert.assertEquals(listenerConfigs.size(), 1);
    assertHttpListener(listenerConfigs.get(0), "0.0.0.0", 9514);

    conf = new PinotConfiguration();
    conf.setProperty("pinot.minion.port", "9513");
    listenerConfigs = ListenerConfigUtil.buildMinionConfigs(conf);
    Assert.assertEquals(listenerConfigs.size(), 1);
    assertHttpListener(listenerConfigs.get(0), "0.0.0.0", 9513);

    conf = new PinotConfiguration();
    conf.setProperty("pinot.minion.adminapi.access.protocols", "https");
    conf.setProperty("pinot.minion.adminapi.access.protocols.https.port", "9512");
    setTlsProperties("pinot.minion.", conf);
    listenerConfigs = ListenerConfigUtil.buildMinionConfigs(conf);
    Assert.assertEquals(listenerConfigs.size(), 1);
    assertHttpsListener(listenerConfigs.get(0), "0.0.0.0", 9512);

    conf = new PinotConfiguration();
    conf.setProperty("pinot.minion.port", "9511");
    conf.setProperty("pinot.minion.adminapi.access.protocols", "https");
    conf.setProperty("pinot.minion.adminapi.access.protocols.https.port", "9510");
    setTlsProperties("pinot.minion.", conf);
    listenerConfigs = ListenerConfigUtil.buildMinionConfigs(conf);
    Assert.assertEquals(listenerConfigs.size(), 2);
    assertHttpListener(listenerConfigs.get(0), "0.0.0.0", 9511);
    assertHttpsListener(listenerConfigs.get(1), "0.0.0.0", 9510);
  }

  private void assertLegacyListener(ListenerConfig legacyListener) {
    assertHttpListener(legacyListener, "0.0.0.0", 9000);
  }

  private void assertHttpListener(ListenerConfig httpsListener, String host, int port) {
    Assert.assertEquals(httpsListener.getName(), "http");
    Assert.assertEquals(httpsListener.getHost(), host);
    Assert.assertEquals(httpsListener.getPort(), port);
    Assert.assertEquals(httpsListener.getProtocol(), "http");
  }

  private void assertHttpsListener(ListenerConfig httpsListener, String host, int port) {
    Assert.assertEquals(httpsListener.getName(), "https");
    Assert.assertEquals(httpsListener.getHost(), host);
    Assert.assertEquals(httpsListener.getPort(), port);
    Assert.assertEquals(httpsListener.getProtocol(), "https");
    Assert.assertNotNull(httpsListener.getTlsConfig());
    Assert.assertEquals(httpsListener.getTlsConfig().getKeyStorePassword(), "a-password");
    Assert.assertEquals(httpsListener.getTlsConfig().getKeyStorePath(), "/some-keystore-path");
    Assert.assertTrue(httpsListener.getTlsConfig().isClientAuthEnabled());
    Assert.assertEquals(httpsListener.getTlsConfig().getTrustStorePassword(), "a-password");
    Assert.assertEquals(httpsListener.getTlsConfig().getTrustStorePath(), "/some-truststore-path");
  }

  private void configureHttpsProperties(PinotConfiguration config, int port) {
    configureHttpsProperties(config, null, port);
  }

  private void configureHttpsProperties(PinotConfiguration config, String host, int port) {
    if (host != null) {
      config.setProperty("controller.access.protocols.https.host", host);
    }
    config.setProperty("controller.access.protocols.https.port", String.valueOf(port));
    setTlsProperties("controller.", config);
  }

  private void setTlsProperties(String prefix, PinotConfiguration config) {
    config.setProperty(prefix + "tls.client.auth.enabled", "true");
    config.setProperty(prefix + "tls.keystore.password", "a-password");
    config.setProperty(prefix + "tls.keystore.path", "/some-keystore-path");
    config.setProperty(prefix + "tls.truststore.password", "a-password");
    config.setProperty(prefix + "tls.truststore.path", "/some-truststore-path");
  }

  private ListenerConfig getListener(String name, List<ListenerConfig> listenerConfigs) {
    return listenerConfigs.stream().filter(listenerConfig -> listenerConfig.getName().equals(name)).findFirst().get();
  }
}
