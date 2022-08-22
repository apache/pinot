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

import java.util.List;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.utils.TlsUtils;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ListenerConfigUtilTest extends TlsUtilsTest {
  @Test
  public void testListenerConfigRef() {
    TlsConfig defaultTls = TlsUtils.extractTlsConfig(_configuration, "pinot.broker.tls");
    Assert.assertNotNull(defaultTls);
    List<ListenerConfig>
        listeners = ListenerConfigUtil.buildListenerConfigs(_configuration, "pinot.broker.client", defaultTls);
    Assert.assertEquals(listeners.size(), 2);
    assertInternalTls(listeners.get(0).getTlsConfig());
    assertExternalTls(listeners.get(1).getTlsConfig());
    Assert.assertEquals(listeners.get(0).getProtocol(), "https");
    Assert.assertEquals(listeners.get(0).getPort(), 8099);
    Assert.assertEquals(listeners.get(1).getPort(), 8443);
  }
}
