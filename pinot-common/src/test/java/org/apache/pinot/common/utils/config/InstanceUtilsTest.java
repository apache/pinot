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
package org.apache.pinot.common.utils.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class InstanceUtilsTest {

  @Test
  public void testToHelixInstanceConfig() {
    Instance instance = new Instance("localhost", 1234, InstanceType.CONTROLLER, null, null, 0, 0, false);
    InstanceConfig instanceConfig = InstanceUtils.toHelixInstanceConfig(instance);
    assertEquals(instanceConfig.getInstanceName(), "Controller_localhost_1234");
    assertTrue(instanceConfig.getInstanceEnabled());
    assertEquals(instanceConfig.getHostName(), "localhost");
    assertEquals(instanceConfig.getPort(), "1234");
    assertTrue(instanceConfig.getTags().isEmpty());
    ZNRecord znRecord = instanceConfig.getRecord();
    assertNull(znRecord.getMapField(InstanceUtils.POOL_KEY));
    assertNull(znRecord.getSimpleField(CommonConstants.Helix.Instance.GRPC_PORT_KEY));
    assertNull(znRecord.getSimpleField(CommonConstants.Helix.Instance.ADMIN_PORT_KEY));
    assertNull(znRecord.getSimpleField(CommonConstants.Helix.QUERIES_DISABLED));

    List<String> tags = Collections.singletonList("DefaultTenant_BROKER");
    instance = new Instance("localhost", 2345, InstanceType.BROKER, tags, null, 0, 0, false);
    instanceConfig = InstanceUtils.toHelixInstanceConfig(instance);
    assertEquals(instanceConfig.getInstanceName(), "Broker_localhost_2345");
    assertTrue(instanceConfig.getInstanceEnabled());
    assertEquals(instanceConfig.getHostName(), "localhost");
    assertEquals(instanceConfig.getPort(), "2345");
    assertEquals(instanceConfig.getTags(), tags);
    znRecord = instanceConfig.getRecord();
    assertNull(znRecord.getMapField(InstanceUtils.POOL_KEY));
    assertNull(znRecord.getSimpleField(CommonConstants.Helix.Instance.GRPC_PORT_KEY));
    assertNull(znRecord.getSimpleField(CommonConstants.Helix.Instance.ADMIN_PORT_KEY));
    assertNull(znRecord.getSimpleField(CommonConstants.Helix.QUERIES_DISABLED));

    tags = Arrays.asList("T1_OFFLINE", "T2_REALTIME");
    Map<String, Integer> poolMap = new TreeMap<>();
    poolMap.put("T1_OFFLINE", 0);
    poolMap.put("T2_REALTIME", 1);
    instance = new Instance("localhost", 3456, InstanceType.SERVER, tags, poolMap, 123, 234, true);
    instanceConfig = InstanceUtils.toHelixInstanceConfig(instance);
    assertEquals(instanceConfig.getInstanceName(), "Server_localhost_3456");
    assertTrue(instanceConfig.getInstanceEnabled());
    assertEquals(instanceConfig.getHostName(), "localhost");
    assertEquals(instanceConfig.getPort(), "3456");
    assertEquals(instanceConfig.getTags(), tags);
    znRecord = instanceConfig.getRecord();
    Map<String, String> expectedPoolMap = new TreeMap<>();
    expectedPoolMap.put("T1_OFFLINE", "0");
    expectedPoolMap.put("T2_REALTIME", "1");
    assertEquals(znRecord.getMapField(InstanceUtils.POOL_KEY), expectedPoolMap);
    assertEquals(znRecord.getSimpleField(CommonConstants.Helix.Instance.GRPC_PORT_KEY), "123");
    assertEquals(znRecord.getSimpleField(CommonConstants.Helix.Instance.ADMIN_PORT_KEY), "234");
    assertEquals(znRecord.getSimpleField(CommonConstants.Helix.QUERIES_DISABLED), "true");

    tags = Collections.singletonList("minion_untagged");
    instance = new Instance("localhost", 4567, InstanceType.MINION, tags, null, 0, 0, false);
    instanceConfig = InstanceUtils.toHelixInstanceConfig(instance);
    assertEquals(instanceConfig.getInstanceName(), "Minion_localhost_4567");
    assertTrue(instanceConfig.getInstanceEnabled());
    assertEquals(instanceConfig.getHostName(), "localhost");
    assertEquals(instanceConfig.getPort(), "4567");
    assertEquals(instanceConfig.getTags(), tags);
    znRecord = instanceConfig.getRecord();
    assertNull(znRecord.getMapField(InstanceUtils.POOL_KEY));
    assertNull(znRecord.getSimpleField(CommonConstants.Helix.Instance.GRPC_PORT_KEY));
    assertNull(znRecord.getSimpleField(CommonConstants.Helix.Instance.ADMIN_PORT_KEY));
    assertNull(znRecord.getSimpleField(CommonConstants.Helix.QUERIES_DISABLED));
  }

  @Test
  public void testUpdateHelixInstanceConfig() {
    Instance instance =
        new Instance("localhost", 1234, InstanceType.SERVER, Collections.singletonList("DefaultTenant_OFFLINE"), null,
            0, 123, false);
    InstanceConfig instanceConfig = InstanceUtils.toHelixInstanceConfig(instance);

    // Put some custom fields, which should not be updated
    ZNRecord znRecord = instanceConfig.getRecord();
    znRecord.setSimpleField("customSimple", "potato");
    List<String> customList = Arrays.asList("foo", "bar");
    znRecord.setListField("customList", customList);
    Map<String, String> customMap = Collections.singletonMap("foo", "bar");
    znRecord.setMapField("customMap", customMap);

    List<String> tags = Arrays.asList("T1_OFFLINE", "T2_REALTIME");
    Map<String, Integer> poolMap = new TreeMap<>();
    poolMap.put("T1_OFFLINE", 0);
    poolMap.put("T2_REALTIME", 1);
    instance = new Instance("myHost", 2345, InstanceType.SERVER, tags, poolMap, 123, 234, true);
    InstanceUtils.updateHelixInstanceConfig(instanceConfig, instance);

    // Instance name should not change
    assertEquals(instanceConfig.getInstanceName(), "Server_localhost_1234");
    assertTrue(instanceConfig.getInstanceEnabled());
    assertEquals(instanceConfig.getHostName(), "myHost");
    assertEquals(instanceConfig.getPort(), "2345");
    assertEquals(instanceConfig.getTags(), tags);
    znRecord = instanceConfig.getRecord();
    Map<String, String> expectedPoolMap = new TreeMap<>();
    expectedPoolMap.put("T1_OFFLINE", "0");
    expectedPoolMap.put("T2_REALTIME", "1");
    assertEquals(znRecord.getMapField(InstanceUtils.POOL_KEY), expectedPoolMap);
    assertEquals(znRecord.getSimpleField(CommonConstants.Helix.Instance.GRPC_PORT_KEY), "123");
    assertEquals(znRecord.getSimpleField(CommonConstants.Helix.Instance.ADMIN_PORT_KEY), "234");
    assertEquals(znRecord.getSimpleField(CommonConstants.Helix.QUERIES_DISABLED), "true");

    // Custom fields should be preserved
    assertEquals(znRecord.getSimpleField("customSimple"), "potato");
    assertEquals(znRecord.getListField("customList"), customList);
    assertEquals(znRecord.getMapField("customMap"), customMap);
  }
}
