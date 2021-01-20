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
package org.apache.pinot.controller.helix.core.minion.generator;

import java.util.Collections;
import java.util.Map;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for {@link SegmentGenerationAndPushTaskGeneratorTest}
 */
public class SegmentGenerationAndPushTaskGeneratorTest extends ControllerTest {
  SegmentGenerationAndPushTaskGenerator _generator;

  @BeforeClass
  public void setup() {
    int zkPort = 2171;
    startZk(zkPort);
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.ZK_STR, "localhost:" + zkPort);
    properties.put(ControllerConf.HELIX_CLUSTER_NAME, SegmentGenerationAndPushTaskGeneratorTest.class.getSimpleName());
    properties.put(ControllerConf.CONTROLLER_PORT, 28998);
    startController(properties);

    ClusterInfoAccessor clusterInfoAccessor = _controllerStarter.getTaskManager().getClusterInfoAccessor();
    _generator = new SegmentGenerationAndPushTaskGenerator();
    _generator.init(clusterInfoAccessor);
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }

  @Test
  public void testRealCluster()
      throws Exception {
    // Default is 1
    Assert.assertEquals(_generator.getNumConcurrentTasksPerInstance(), 1);

    // Set config to 5
    String request = JsonUtils.objectToString(Collections
        .singletonMap(MinionConstants.SegmentGenerationAndPushTask.CONFIG_NUMBER_CONCURRENT_TASKS_PER_INSTANCE, "5"));
    sendPostRequest(_controllerRequestURLBuilder.forClusterConfigs(), request);
    Assert.assertEquals(_generator.getNumConcurrentTasksPerInstance(), 5);

    // Set config to invalid and should still get 1
    request = JsonUtils.objectToString(Collections
        .singletonMap(MinionConstants.SegmentGenerationAndPushTask.CONFIG_NUMBER_CONCURRENT_TASKS_PER_INSTANCE,
            "abcd"));
    sendPostRequest(_controllerRequestURLBuilder.forClusterConfigs(), request);
    Assert.assertEquals(_generator.getNumConcurrentTasksPerInstance(), 1);
  }
}
