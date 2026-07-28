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
package org.apache.pinot.server.starter;

import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.server.conf.ServerConf;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;
import static org.apache.pinot.spi.utils.CommonConstants.Server.METRICS_CONFIG_PREFIX;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;


public class ServerMetricsInitUtilsTest {

  @AfterMethod
  public void tearDown() {
    ServerMetrics.deregister();
  }

  private static ServerConf buildServerConf() {
    PinotConfiguration pinotConfig = new PinotConfiguration();
    pinotConfig.setProperty(METRICS_CONFIG_PREFIX + "." + CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    return new ServerConf(pinotConfig);
  }

  @Test
  public void testInitServerMetricsConstructsAndRegisters()
      throws Exception {
    ServerMetrics serverMetrics = ServerMetricsInitUtils.initServerMetrics(buildServerConf());

    assertNotNull(serverMetrics);
    assertSame(ServerMetrics.get(), serverMetrics, "Returned ServerMetrics should be the registered instance");
  }

  @Test
  public void testInitServerMetricsReturnsExistingWhenAlreadyRegistered()
      throws Exception {
    ServerConf serverConf = buildServerConf();
    ServerMetrics first = ServerMetricsInitUtils.initServerMetrics(serverConf);
    ServerMetrics second = ServerMetricsInitUtils.initServerMetrics(serverConf);

    assertSame(second, first, "Should return the already-registered instance");
  }
}
