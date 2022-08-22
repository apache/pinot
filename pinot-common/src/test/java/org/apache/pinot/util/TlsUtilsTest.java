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
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TlsUtilsTest {

  protected PinotConfiguration _configuration = new PinotConfiguration();

  @BeforeTest
  public void loadConfig() throws Exception {
    PropertiesConfiguration props = new PropertiesConfiguration();
    props.load(TlsUtilsTest.class.getClassLoader().getResourceAsStream("sample_broker_config.properties"));
    _configuration = new PinotConfiguration(props);
  }

  @Test
  public void testTlsSettingsReference() throws Exception {

    TlsConfig internalTls =
        TlsUtils.extractTlsConfig(_configuration, "pinot.broker.client.access.protocols.internal.tls");
    assertInternalTls(internalTls);

    TlsConfig externalTls =
        TlsUtils.extractTlsConfig(_configuration, "pinot.broker.client.access.protocols.external.tls");
    assertExternalTls(externalTls);

    TlsConfig brokerTls = TlsUtils.extractTlsConfig(_configuration, "pinot.broker.tls");
    assertInternalTls(brokerTls);
  }

  protected void assertExternalTls(TlsConfig externalTls) {
    Assert.assertEquals(externalTls.getKeyStorePath(), "/home/tls-store/keystore-external.jks");
    Assert.assertEquals(externalTls.getKeyStorePassword(), "third-secret");
    Assert.assertEquals(externalTls.getKeyStoreType(), "PKCS");
    Assert.assertFalse(externalTls.isClientAuthEnabled());
    Assert.assertEquals(externalTls.getTrustStorePath(), "/home/tls-store/truststore-external.jks");
    Assert.assertEquals(externalTls.getTrustStorePassword(), "forth-secret");
    Assert.assertEquals(externalTls.getTrustStoreType(), "PKCS");
  }

  protected void assertInternalTls(TlsConfig internalTls) {
    Assert.assertEquals(internalTls.getKeyStorePath(), "/home/tls-store/keystore-internal.jks");
    Assert.assertEquals(internalTls.getKeyStorePassword(), "first-secret");
    Assert.assertEquals(internalTls.getKeyStoreType(), "JKS");
    Assert.assertTrue(internalTls.isClientAuthEnabled());
    Assert.assertEquals(internalTls.getTrustStorePath(), "/home/tls-store/truststore-internal.jks");
    Assert.assertEquals(internalTls.getTrustStorePassword(), "second-secret");
    Assert.assertEquals(internalTls.getTrustStoreType(), "JKS");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testTlsSettingsSelfReference()
      throws Exception {
    TlsConfig internalTls = TlsUtils.extractTlsConfig(_configuration, "pinot.shared.tls.bad_setting");
    Assert.fail("should throw exception for circular reference");
  }
}
