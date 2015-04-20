/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.perf;

import java.util.List;

import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPerfBenchMarkDriver {

  @Test
  public void testPerfClusterSetup() throws Exception {
    PerfBenchmarkDriverConf conf = new PerfBenchmarkDriverConf();
    conf.setUploadIndexes(true);
    conf.setConfigureResources(true);
    PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    driver.run();
    ZKHelixAdmin helixAdmin = new ZKHelixAdmin(conf.getZkHost() + ":" + conf.getZkPort());
    //Ensure that there are two instances created, one broker and one server
    List<String> instancesInCluster = helixAdmin.getInstancesInCluster(conf.getClusterName());
    Assert.assertEquals(instancesInCluster.size(), 2);
    //Ensure that BrokerResource is created 
    List<String> resourcesInCluster = helixAdmin.getResourcesInCluster(conf.getClusterName());
    Assert.assertTrue(resourcesInCluster.contains("brokerResource"));

  }

}
