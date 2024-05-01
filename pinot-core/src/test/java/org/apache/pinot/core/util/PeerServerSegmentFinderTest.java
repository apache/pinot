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
import java.util.List;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.Instance;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class PeerServerSegmentFinderTest {
  private static final String CLUSTER_NAME = "testCluster";
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
  private static final String SEGMENT_1 = "testSegment1";
  private static final String SEGMENT_2 = "testSegment2";
  private static final String INSTANCE_ID_1 = "Server_s1_1007";
  private static final String INSTANCE_ID_2 = "Server_s2_1007";
  private static final String INSTANCE_ID_3 = "Server_s3_1007";
  private static final String HOSTNAME_1 = "s1";
  private static final String HOSTNAME_2 = "s2";
  private static final String HOSTNAME_3 = "s3";
  private static final int HELIX_PORT = 1007;
  private static final int HTTP_ADMIN_PORT = 1008;
  private static final int HTTPS_ADMIN_PORT = 1009;

  private HelixManager _helixManager;

  @BeforeClass
  public void initSegmentFetcherFactoryWithPeerServerSegmentFetcher() {
    ExternalView externalView = new ExternalView(REALTIME_TABLE_NAME);
    externalView.setState(SEGMENT_1, INSTANCE_ID_1, "ONLINE");
    externalView.setState(SEGMENT_1, INSTANCE_ID_2, "OFFLINE");
    externalView.setState(SEGMENT_1, INSTANCE_ID_3, "ONLINE");
    externalView.setState(SEGMENT_2, INSTANCE_ID_1, "OFFLINE");
    externalView.setState(SEGMENT_2, INSTANCE_ID_2, "OFFLINE");

    _helixManager = mock(HelixManager.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(_helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(_helixManager.getClusterName()).thenReturn(CLUSTER_NAME);
    when(helixAdmin.getResourceExternalView(CLUSTER_NAME, REALTIME_TABLE_NAME)).thenReturn(externalView);
    when(helixAdmin.getInstanceConfig(CLUSTER_NAME, INSTANCE_ID_1)).thenReturn(
        getInstanceConfig(INSTANCE_ID_1, HOSTNAME_1));
    when(helixAdmin.getInstanceConfig(CLUSTER_NAME, INSTANCE_ID_2)).thenReturn(
        getInstanceConfig(INSTANCE_ID_2, HOSTNAME_2));
    when(helixAdmin.getInstanceConfig(CLUSTER_NAME, INSTANCE_ID_3)).thenReturn(
        getInstanceConfig(INSTANCE_ID_3, HOSTNAME_3));
  }

  private static InstanceConfig getInstanceConfig(String instanceId, String hostName) {
    InstanceConfig instanceConfig = new InstanceConfig(instanceId);
    instanceConfig.setHostName(hostName);
    instanceConfig.setPort(Integer.toString(HELIX_PORT));
    instanceConfig.getRecord().setIntField(Instance.ADMIN_PORT_KEY, HTTP_ADMIN_PORT);
    instanceConfig.getRecord().setIntField(Instance.ADMIN_HTTPS_PORT_KEY, HTTPS_ADMIN_PORT);
    return instanceConfig;
  }

  @Test
  public void testSegmentFoundSuccessfully()
      throws Exception {
    // SEGMENT_1 has only 2 online replicas.
    List<URI> httpServerURIs = PeerServerSegmentFinder.getPeerServerURIs(_helixManager, REALTIME_TABLE_NAME, SEGMENT_1,
        CommonConstants.HTTP_PROTOCOL);
    assertEquals(httpServerURIs.size(), 2);
    assertTrue(httpServerURIs.contains(new URI(
        String.format("http://%s:%d/segments/%s/%s", HOSTNAME_1, HTTP_ADMIN_PORT, REALTIME_TABLE_NAME, SEGMENT_1))));
    assertTrue(httpServerURIs.contains(new URI(
        String.format("http://%s:%d/segments/%s/%s", HOSTNAME_3, HTTP_ADMIN_PORT, REALTIME_TABLE_NAME, SEGMENT_1))));
    List<URI> httpsServerURIs = PeerServerSegmentFinder.getPeerServerURIs(_helixManager, REALTIME_TABLE_NAME, SEGMENT_1,
        CommonConstants.HTTPS_PROTOCOL);
    assertEquals(httpsServerURIs.size(), 2);
    assertTrue(httpsServerURIs.contains(new URI(
        String.format("https://%s:%d/segments/%s/%s", HOSTNAME_1, HTTPS_ADMIN_PORT, REALTIME_TABLE_NAME, SEGMENT_1))));
    assertTrue(httpsServerURIs.contains(new URI(
        String.format("https://%s:%d/segments/%s/%s", HOSTNAME_3, HTTPS_ADMIN_PORT, REALTIME_TABLE_NAME, SEGMENT_1))));
  }

  @Test
  public void testSegmentNotFound() {
    assertTrue(PeerServerSegmentFinder.getPeerServerURIs(_helixManager, REALTIME_TABLE_NAME, SEGMENT_2,
        CommonConstants.HTTP_PROTOCOL).isEmpty());
  }
}
