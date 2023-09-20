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
package org.apache.pinot.core.util;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.StringUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class PeerServerSegmentFinderTest {
  private static final String TABLE_NAME_WITH_TYPE = "testTable_REALTIME";
  private static final String SEGMENT_1 = "testTable__0__0__t11";
  private static final String SEGMENT_2 = "testTable__0__1__t11";
  private static final String CLUSTER_NAME = "dummyCluster";
  private static final String INSTANCE_ID1 = "Server_localhost_1000";
  private static final String INSTANCE_ID2 = "Server_localhost_1001";
  private static final String INSTANCE_ID3 = "Server_localhost_1003";
  public static final String ADMIN_PORT = "1008";
  public static final String HOST_1_NAME = "s1";
  public static final String HOST_2_NAME = "s2";
  public static final String HOST_3_NAME = "s3";
  private HelixManager _helixManager;

  @BeforeClass
  public void initSegmentFetcherFactoryWithPeerServerSegmentFetcher()
      throws Exception {
    HelixAdmin helixAdmin;
    {
      ExternalView ev = new ExternalView(TABLE_NAME_WITH_TYPE);
      ev.setState(SEGMENT_1, INSTANCE_ID1, "ONLINE");
      ev.setState(SEGMENT_1, INSTANCE_ID2, "OFFLINE");
      ev.setState(SEGMENT_1, INSTANCE_ID3, "ONLINE");
      ev.setState(SEGMENT_2, INSTANCE_ID1, "OFFLINE");
      ev.setState(SEGMENT_2, INSTANCE_ID2, "OFFLINE");
      _helixManager = mock(HelixManager.class);
      helixAdmin = mock(HelixAdmin.class);
      when(_helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
      when(_helixManager.getClusterName()).thenReturn(CLUSTER_NAME);
      when(helixAdmin.getResourceExternalView(CLUSTER_NAME, TABLE_NAME_WITH_TYPE)).thenReturn(ev);
      when(helixAdmin.getConfigKeys(any(HelixConfigScope.class))).thenReturn(new ArrayList<>());
      Map<String, String> instanceConfigMap = new HashMap<>();
      instanceConfigMap.put(CommonConstants.Helix.Instance.ADMIN_PORT_KEY, ADMIN_PORT);
      when(helixAdmin.getConfig(any(HelixConfigScope.class), any(List.class))).thenReturn(instanceConfigMap);
      InstanceConfig instanceConfig1 = new InstanceConfig(INSTANCE_ID1);
      instanceConfig1.setHostName(HOST_1_NAME);
      instanceConfig1.setPort("1000");
      when(helixAdmin.getInstanceConfig(any(String.class), eq(INSTANCE_ID1))).thenReturn(instanceConfig1);

      InstanceConfig instanceConfig2 = new InstanceConfig(INSTANCE_ID2);
      instanceConfig2.setHostName(HOST_2_NAME);
      instanceConfig2.setPort("1000");
      when(helixAdmin.getInstanceConfig(any(String.class), eq(INSTANCE_ID2))).thenReturn(instanceConfig2);

      InstanceConfig instanceConfig3 = new InstanceConfig(INSTANCE_ID3);
      instanceConfig3.setHostName(HOST_3_NAME);
      instanceConfig3.setPort("1000");
      when(helixAdmin.getInstanceConfig(any(String.class), eq(INSTANCE_ID3))).thenReturn(instanceConfig3);
    }
  }

  @Test
  public void testSegmentFoundSuccessfully()
      throws Exception {
    // SEGMENT_1 has only 2 online replicas.
    List<URI> httpServerURIs =
        PeerServerSegmentFinder.getPeerServerURIs(SEGMENT_1, CommonConstants.HTTP_PROTOCOL, _helixManager,
            TABLE_NAME_WITH_TYPE);
    assertEquals(2, httpServerURIs.size());
    httpServerURIs.contains(new URI(
        StringUtil.join("/", "http://" + HOST_1_NAME + ":" + ADMIN_PORT, "segments", TABLE_NAME_WITH_TYPE, SEGMENT_1)));
    httpServerURIs.contains(new URI(
        StringUtil.join("/", "http://" + HOST_3_NAME + ":" + ADMIN_PORT, "segments", TABLE_NAME_WITH_TYPE, SEGMENT_1)));
    List<URI> httpsServerURIs =
        PeerServerSegmentFinder.getPeerServerURIs(SEGMENT_1, CommonConstants.HTTPS_PROTOCOL, _helixManager,
            TABLE_NAME_WITH_TYPE);
    assertEquals(2, httpsServerURIs.size());
    httpServerURIs.contains(new URI(
        StringUtil.join("/", "https://" + HOST_1_NAME + ":" + ADMIN_PORT, "segments", TABLE_NAME_WITH_TYPE,
            SEGMENT_1)));
    httpServerURIs.contains(new URI(
        StringUtil.join("/", "https://" + HOST_3_NAME + ":" + ADMIN_PORT, "segments", TABLE_NAME_WITH_TYPE,
            SEGMENT_1)));
  }

  @Test
  public void testSegmentNotFound()
      throws Exception {
    Assert.assertEquals(0,
        PeerServerSegmentFinder.getPeerServerURIs(SEGMENT_2, CommonConstants.HTTP_PROTOCOL, _helixManager,
            TABLE_NAME_WITH_TYPE).size());
  }
}
