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
package org.apache.pinot.common.utils.grpc;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.utils.TlsUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


public class GrpcQueryClientTest {
  private static final String PASSWORD1 = "very secret";
  private static final String PASSWORD2 = "i am a secret";
  private static final String INTENTIONALLY_NOT_JKS = "not_jks";
  private static final String INTENTIONALLY_NOT_PKCS12 = "not_pkcs12";
  private static final String FILE1 = "file://file1";
  private static final String FILE2 = "file://file2";

  @Test
  public void testConfigParsing() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("usePlainText", "false");
    configMap.put("tls.keystore.path", FILE1);
    configMap.put("tls.keystore.password", PASSWORD1);
    configMap.put("tls.keystore.type", INTENTIONALLY_NOT_JKS);
    configMap.put("tls.truststore.path", FILE2);
    configMap.put("tls.truststore.password", PASSWORD2);
    configMap.put("tls.truststore.type", INTENTIONALLY_NOT_PKCS12);
    configMap.put("tls.ssl.provider", "JDK");
    GrpcQueryClient.Config config = new GrpcQueryClient.Config(configMap);
    Assert.assertFalse(config.isUsePlainText());
    Assert.assertEquals(PASSWORD1, config.getTlsConfig().getKeyStorePassword());
    Assert.assertEquals(PASSWORD2, config.getTlsConfig().getTrustStorePassword());
    Assert.assertEquals(FILE1, config.getTlsConfig().getKeyStorePath());
    Assert.assertEquals(FILE2, config.getTlsConfig().getTrustStorePath());
    Assert.assertEquals(INTENTIONALLY_NOT_JKS, config.getTlsConfig().getKeyStoreType());
    Assert.assertEquals(INTENTIONALLY_NOT_PKCS12, config.getTlsConfig().getTrustStoreType());
  }

  @Test
  public void testConfigNamespaceParsing() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("my.prefix.usePlainText", "false");
    configMap.put("my.prefix.tls.keystore.path", FILE1);
    configMap.put("my.prefix.tls.keystore.password", PASSWORD1);
    configMap.put("my.prefix.tls.keystore.type", INTENTIONALLY_NOT_JKS);
    configMap.put("my.prefix.tls.truststore.path", FILE2);
    configMap.put("my.prefix.tls.truststore.password", PASSWORD2);
    configMap.put("my.prefix.tls.truststore.type", INTENTIONALLY_NOT_PKCS12);
    configMap.put("my.prefix.tls.ssl.provider", "JDK");
    GrpcQueryClient.Config config = new GrpcQueryClient.Config(configMap, "my.prefix");
    Assert.assertFalse(config.isUsePlainText());
    Assert.assertEquals(PASSWORD1, config.getTlsConfig().getKeyStorePassword());
    Assert.assertEquals(PASSWORD2, config.getTlsConfig().getTrustStorePassword());
    Assert.assertEquals(FILE1, config.getTlsConfig().getKeyStorePath());
    Assert.assertEquals(FILE2, config.getTlsConfig().getTrustStorePath());
    Assert.assertEquals(INTENTIONALLY_NOT_JKS, config.getTlsConfig().getKeyStoreType());
    Assert.assertEquals(INTENTIONALLY_NOT_PKCS12, config.getTlsConfig().getTrustStoreType());
  }

  @Test
  public void testPassingInTls() {
    Map<String, Object> brokerConfigMap = new HashMap<>();
    brokerConfigMap.put("broker.tls.keystore.path", FILE2);
    brokerConfigMap.put("broker.tls.keystore.password", PASSWORD2);
    brokerConfigMap.put("broker.tls.keystore.type", INTENTIONALLY_NOT_PKCS12);
    brokerConfigMap.put("broker.tls.truststore.path", FILE1);
    brokerConfigMap.put("broker.tls.truststore.password", PASSWORD1);
    brokerConfigMap.put("broker.tls.truststore.type", INTENTIONALLY_NOT_JKS);
    brokerConfigMap.put("broker.tls.ssl.provider", "JDK");
    TlsConfig brokerTls = TlsUtils.extractTlsConfig(new PinotConfiguration(brokerConfigMap), "broker.tls");

    Map<String, Object> configMap = new HashMap<>();
    configMap.put("my.prefix.usePlainText", "false");
    configMap.put("my.prefix.tls.keystore.path", FILE1);
    configMap.put("my.prefix.tls.keystore.password", PASSWORD1);
    configMap.put("my.prefix.tls.keystore.type", INTENTIONALLY_NOT_JKS);
    configMap.put("my.prefix.tls.truststore.path", FILE2);
    configMap.put("my.prefix.tls.truststore.password", PASSWORD2);
    configMap.put("my.prefix.tls.truststore.type", INTENTIONALLY_NOT_PKCS12);
    configMap.put("my.prefix.tls.ssl.provider", "JDK");
    GrpcQueryClient.Config config = new GrpcQueryClient.Config(configMap, "my.prefix", brokerTls);
    Assert.assertFalse(config.isUsePlainText());
    Assert.assertEquals(PASSWORD2, config.getTlsConfig().getKeyStorePassword());
    Assert.assertEquals(PASSWORD1, config.getTlsConfig().getTrustStorePassword());
    Assert.assertEquals(FILE2, config.getTlsConfig().getKeyStorePath());
    Assert.assertEquals(FILE1, config.getTlsConfig().getTrustStorePath());
    Assert.assertEquals(INTENTIONALLY_NOT_PKCS12, config.getTlsConfig().getKeyStoreType());
    Assert.assertEquals(INTENTIONALLY_NOT_JKS, config.getTlsConfig().getTrustStoreType());
  }
}
