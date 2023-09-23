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
package org.apache.pinot.integration.tests;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.testng.annotations.Test;


@Test(suiteName = "integration-suite-2", groups = {"integration-suite-2"})
public class OfflineSecureGRPCServerIntegrationTest extends OfflineGRPCServerIntegrationTest {
  private static final String JKS = "JKS";
  private static final String JDK = "JDK";
  private static final String PASSWORD = "changeit";
  private final URL _tlsStoreJKS = OfflineSecureGRPCServerIntegrationTest.class.getResource("/tlstest.jks");

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(Server.CONFIG_OF_GRPCTLS_SERVER_ENABLED, true);
    serverConf.setProperty("pinot.server.grpctls.client.auth.enabled", true);
    serverConf.setProperty("pinot.server.grpctls.keystore.type", JKS);
    serverConf.setProperty("pinot.server.grpctls.keystore.path", _tlsStoreJKS);
    serverConf.setProperty("pinot.server.grpctls.keystore.password", PASSWORD);
    serverConf.setProperty("pinot.server.grpctls.truststore.type", JKS);
    serverConf.setProperty("pinot.server.grpctls.truststore.path", _tlsStoreJKS);
    serverConf.setProperty("pinot.server.grpctls.truststore.password", PASSWORD);
    serverConf.setProperty("pinot.server.grpctls.ssl.provider", JDK);
  }

  @Override
  protected GrpcQueryClient getGrpcQueryClient() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("usePlainText", "false");
    configMap.put("tls.keystore.path", _tlsStoreJKS.getFile());
    configMap.put("tls.keystore.password", PASSWORD);
    configMap.put("tls.keystore.type", JKS);
    configMap.put("tls.truststore.path", _tlsStoreJKS.getFile());
    configMap.put("tls.truststore.password", PASSWORD);
    configMap.put("tls.truststore.type", JKS);
    configMap.put("tls.ssl.provider", JDK);
    PinotConfiguration brokerConfig = new PinotConfiguration(configMap);
    // This mimics how pinot broker instantiates GRPCQueryClient.
    GrpcConfig config = GrpcConfig.buildGrpcQueryConfig(brokerConfig);
    return new GrpcQueryClient("localhost", getServerGrpcPort(), config);
  }
}
