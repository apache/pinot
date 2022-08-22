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
package org.apache.pinot.util;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.utils.TlsUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TlsUtilsTest {

  @Test
  public void testTlsSettingsReference() throws Exception {
    PropertiesConfiguration props = new PropertiesConfiguration();
    props.load(TlsUtilsTest.class.getClassLoader().getResourceAsStream("sample_broker_config.properties"));
    PinotConfiguration configuration = new PinotConfiguration(props);
    TlsConfig internalTls =
        TlsUtils.extractTlsConfig(configuration, "pinot.broker.client.access.protocols.internal.tls");
    Assert.assertEquals(internalTls.getKeyStorePath(), "/home/tls-store/keystore-internal.jks");
    Assert.assertEquals(internalTls.getKeyStorePassword(), "first-secret");
    Assert.assertEquals(internalTls.getKeyStoreType(), "JKS");
    Assert.assertTrue(internalTls.isClientAuthEnabled());
    Assert.assertEquals(internalTls.getTrustStorePath(), "/home/tls-store/truststore-internal.jks");
    Assert.assertEquals(internalTls.getTrustStorePassword(), "second-secret");
    Assert.assertEquals(internalTls.getTrustStoreType(), "JKS");

    TlsConfig externalTls =
        TlsUtils.extractTlsConfig(configuration, "pinot.broker.client.access.protocols.external.tls");
    Assert.assertEquals(externalTls.getKeyStorePath(), "/home/tls-store/keystore-external.jks");
    Assert.assertEquals(externalTls.getKeyStorePassword(), "third-secret");
    Assert.assertEquals(externalTls.getKeyStoreType(), "PKCS");
    Assert.assertFalse(externalTls.isClientAuthEnabled());
    Assert.assertEquals(externalTls.getTrustStorePath(), "/home/tls-store/truststore-external.jks");
    Assert.assertEquals(externalTls.getTrustStorePassword(), "forth-secret");
    Assert.assertEquals(externalTls.getTrustStoreType(), "PKCS");

    TlsConfig brokerTls = TlsUtils.extractTlsConfig(configuration, "pinot.broker.tls");
    Assert.assertEquals(brokerTls.getKeyStorePath(), "/home/tls-store/keystore-internal.jks");
    Assert.assertEquals(brokerTls.getKeyStorePassword(), "first-secret");
    Assert.assertEquals(brokerTls.getKeyStoreType(), "JKS");
    Assert.assertTrue(brokerTls.isClientAuthEnabled());
    Assert.assertEquals(brokerTls.getTrustStorePath(), "/home/tls-store/truststore-internal.jks");
    Assert.assertEquals(brokerTls.getTrustStorePassword(), "second-secret");
    Assert.assertEquals(brokerTls.getTrustStoreType(), "JKS");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testTlsSettingsSelfReference()
      throws Exception {
    PropertiesConfiguration props = new PropertiesConfiguration();
    props.load(TlsUtilsTest.class.getClassLoader().getResourceAsStream("sample_broker_config.properties"));
    PinotConfiguration configuration = new PinotConfiguration(props);
    TlsConfig internalTls = TlsUtils.extractTlsConfig(configuration, "pinot.shared.tls.bad_setting");
    Assert.fail("should throw exception for circular reference");
  }
}
