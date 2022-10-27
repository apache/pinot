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
package org.apache.pinot.controller.api.resources;

import java.util.Map;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.utils.StringUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerConf.CONTROLLER_RESOURCE_PACKAGES;
import static org.apache.pinot.controller.ControllerConf.DEFAULT_CONTROLLER_RESOURCE_PACKAGES;
import static org.testng.Assert.assertEquals;


/**
 * Test for extra resource package registered with HTTP server
 */
@Test(groups = "stateless")
public class PinotDummyExtraRestletResourceStatelessTest extends ControllerTest {

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(CONTROLLER_RESOURCE_PACKAGES,
        DEFAULT_CONTROLLER_RESOURCE_PACKAGES + ",org.apache.pinot.controller.api.extraresources");
    startController(properties);
  }

  @Test
  public void testExtraDummyResourcePackages()
      throws Exception {
    String resp = ControllerTest.sendGetRequest(StringUtil.join("/", getControllerBaseApiUrl(), "testExtra"));
    assertEquals(resp, "DummyMsg");
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
