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
package org.apache.pinot.controller.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.pinot.common.utils.PinotAppConfigs;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test for {@link org.apache.pinot.controller.api.resources.PinotControllerAppConfigs} class.
 */
public class PinotControllerAppConfigsTest {
  @BeforeClass
  public void setUp()
      throws Exception {
    ControllerTestUtils.setupClusterAndValidate();
  }

  /**
   * Asserts that app configurations returned by the controller endpoint
   * are as expected.
   * @throws IOException In case of exception
   */
  @Test
  public void testControllerAppConfigs()
      throws IOException {
    ControllerConf expectedControllerConf = ControllerTestUtils.getControllerConfig();
    PinotAppConfigs expected = new PinotAppConfigs(expectedControllerConf);

    String configsJson =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forAppConfigs());
    PinotAppConfigs actual = new ObjectMapper().readValue(configsJson, PinotAppConfigs.class);

    // RuntimeConfig is not checked as it has information that can change during the test run.
    // Also, some of the system configs can change, so compare the ones that don't.
    PinotAppConfigs.SystemConfig actualSystemConfig = actual.getSystemConfig();
    PinotAppConfigs.SystemConfig expectedSystemConfig = expected.getSystemConfig();

    Assert.assertEquals(actualSystemConfig.getName(), expectedSystemConfig.getName());
    Assert.assertEquals(actualSystemConfig.getVersion(), expectedSystemConfig.getVersion());
    Assert.assertEquals(actualSystemConfig.getAvailableProcessors(), expectedSystemConfig.getAvailableProcessors());
    Assert.assertEquals(actualSystemConfig.getTotalPhysicalMemory(), expectedSystemConfig.getTotalPhysicalMemory());
    Assert.assertEquals(actualSystemConfig.getTotalSwapSpace(), expectedSystemConfig.getTotalSwapSpace());

    Assert.assertEquals(actual.getJvmConfig(), expected.getJvmConfig());
    Assert.assertEquals(actual.getPinotConfig(), expectedControllerConf.toMap());
  }

  @AfterClass
  public void tearDown() {
    ControllerTestUtils.cleanup();
  }
}
