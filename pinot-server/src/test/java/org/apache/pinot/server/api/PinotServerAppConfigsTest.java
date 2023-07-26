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
package org.apache.pinot.server.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.net.SocketException;
import java.net.UnknownHostException;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.utils.PinotAppConfigs;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.Obfuscator;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link org.apache.pinot.server.api.resources.PinotServerAppConfigs} class.
 */
public class PinotServerAppConfigsTest extends BaseResourceTest {

  /**
   * Asserts that application configs returned by the server endpoint are as expected.
   *
   * @throws JsonProcessingException In case an exception is encountered during JSON processing.
   */
  @Test
  public void testAppConfigs()
      throws JsonProcessingException, SocketException, UnknownHostException {
    PinotConfiguration expectedServerConf = new PinotConfiguration();
    String hostname = expectedServerConf.getProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST,
        expectedServerConf.getProperty(CommonConstants.Helix.SET_INSTANCE_ID_TO_HOSTNAME_KEY, false)
            ? NetUtils.getHostnameOrAddress() : NetUtils.getHostAddress());
    int port = expectedServerConf.getProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT,
        CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT);
    expectedServerConf.setProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_ID,
        CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + hostname + "_" + port);
    PinotAppConfigs expected = new PinotAppConfigs(expectedServerConf);

    Response response = _webTarget.path("/appconfigs").request().get(Response.class);
    String configsJson = response.readEntity(String.class);
    PinotAppConfigs actual = JsonUtils.stringToObject(configsJson, PinotAppConfigs.class);

    // RuntimeConfig is not checked as it has information that can change during the test run.
    // Also, some of the system configs can change, so compare the ones that don't.
    PinotAppConfigs.SystemConfig actualSystemConfig = actual.getSystemConfig();
    PinotAppConfigs.SystemConfig expectedSystemConfig = expected.getSystemConfig();

    Assert.assertEquals(actualSystemConfig.getName(), expectedSystemConfig.getName());
    Assert.assertEquals(actualSystemConfig.getVersion(), expectedSystemConfig.getVersion());
    Assert.assertEquals(actualSystemConfig.getAvailableProcessors(), expectedSystemConfig.getAvailableProcessors());
    Assert.assertEquals(actualSystemConfig.getTotalPhysicalMemory(), expectedSystemConfig.getTotalPhysicalMemory());
    Assert.assertEquals(actualSystemConfig.getTotalSwapSpace(), expectedSystemConfig.getTotalSwapSpace());

    // tests Equals on obfuscated expected and actual
    Obfuscator obfuscator = new Obfuscator();
    String obfuscatedExpectedJson = obfuscator.toJsonString(expected);
    PinotAppConfigs obfuscatedExpected = JsonUtils.stringToObject(obfuscatedExpectedJson, PinotAppConfigs.class);
    Assert.assertEquals(actual.getJvmConfig(), obfuscatedExpected.getJvmConfig());
    Assert.assertEquals(actual.getPinotConfig(), obfuscatedExpected.getPinotConfig());
  }
}
