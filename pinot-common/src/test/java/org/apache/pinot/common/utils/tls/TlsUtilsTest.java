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
package org.apache.pinot.common.utils.tls;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TlsUtilsTest {
  @Test
  public void installDefaultSSLSocketFactoryWhenNoKeyOrTrustStore() {
    TlsUtils.installDefaultSSLSocketFactory(null, null, null, null, null, null);
  }

  @Test
  public void extractTlsConfigShouldTrimProtocols() {
    Map<String, Object> props = new HashMap<>();
    props.put("tls.protocols", " TLSv1.2, TLSv1.3 ,, ");
    TlsConfig tlsConfig = TlsUtils.extractTlsConfig(new PinotConfiguration(props), "tls");
    Assert.assertEquals(tlsConfig.getAllowedProtocols(), new String[]{"TLSv1.2", "TLSv1.3"});
  }

  @Test
  public void extractTlsConfigShouldFallbackToDefaultProtocolsWhenEmpty() {
    Map<String, Object> props = new HashMap<>();
    props.put("tls.protocols", " , ");
    TlsConfig defaultConfig = new TlsConfig();
    defaultConfig.setAllowedProtocols(new String[]{"TLSv1.2"});
    TlsConfig tlsConfig = TlsUtils.extractTlsConfig(new PinotConfiguration(props), "tls", defaultConfig);
    Assert.assertEquals(tlsConfig.getAllowedProtocols(), new String[]{"TLSv1.2"});
  }
}
