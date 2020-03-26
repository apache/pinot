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
package org.apache.pinot.common.utils.fetcher;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.StringUtil;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class PeerServerSegmentFetcherTest {
  private static final String HTTP_PROTOCOL = "http";
  private static final String HTTPS_PROTOCOL = "https";
  private static final String PEER_2_PEER_PROTOCOL = "server";

  private static final String TABLE_NAME_WITH_TYPE = "testTable_REALTIME";
  private static final String SEGMENT_1 = "testTable__0__0__t11";
  private static final String SEGMENT_2 = "testTable__0__1__t11";
  private static final String CLUSTER_NAME = "dummyCluster";
  private static final String INSTANCE_ID1 = "Server_localhost_1000";
  private static final String INSTANCE_ID2 = "Server_localhost_1001";

  @BeforeClass
  public void initSegmentFetcherFactoryWithPeerServerSegmentFetcher()
      throws Exception {
    Configuration config = new BaseConfiguration();
    config.addProperty("protocols", Arrays.asList(HTTP_PROTOCOL, HTTPS_PROTOCOL, PEER_2_PEER_PROTOCOL));
    config.addProperty(HTTP_PROTOCOL + SegmentFetcherFactory.SEGMENT_FETCHER_CLASS_KEY_SUFFIX,
        FakeHttpSegmentFetcher.class.getName());

    HelixManager helixManager;
    HelixAdmin helixAdmin;
    {
      ExternalView ev = new ExternalView(TABLE_NAME_WITH_TYPE);
      ev.setState(SEGMENT_1, INSTANCE_ID1, "ONLINE");
      ev.setState(SEGMENT_1, INSTANCE_ID2, "OFFLINE");
      ev.setState(SEGMENT_2, INSTANCE_ID1, "OFFLINE");
      ev.setState(SEGMENT_2, INSTANCE_ID2, "OFFLINE");
      helixManager = mock(HelixManager.class);
      helixAdmin = mock(HelixAdmin.class);
      when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
      when(helixAdmin.getResourceExternalView(CLUSTER_NAME, TABLE_NAME_WITH_TYPE)).thenReturn(ev);
      when(helixAdmin.getConfigKeys(any(HelixConfigScope.class))).thenReturn(new ArrayList<>());
      Map<String, String> instanceConfigMap = new HashMap<>();
      instanceConfigMap.put(CommonConstants.Helix.Instance.ADMIN_PORT_KEY, "1008");
      when(helixAdmin.getConfig(any(HelixConfigScope.class), any(List.class))).thenReturn(instanceConfigMap);
      InstanceConfig instanceConfig1 = new InstanceConfig(INSTANCE_ID1);
      instanceConfig1.setHostName("s1");
      instanceConfig1.setPort("1000");
      when(helixAdmin.getInstanceConfig(any(String.class), eq(INSTANCE_ID1))).thenReturn(instanceConfig1);

      InstanceConfig instanceConfig2 = new InstanceConfig(INSTANCE_ID2);
      instanceConfig2.setHostName("s2");
      instanceConfig2.setPort("1000");
      when(helixAdmin.getInstanceConfig(any(String.class), eq(INSTANCE_ID2))).thenReturn(instanceConfig2);
    }

    SegmentFetcherFactory.init(config, helixManager, CLUSTER_NAME);

    assertEquals(SegmentFetcherFactory.getSegmentFetcher(HTTP_PROTOCOL).getClass(), FakeHttpSegmentFetcher.class);
    assertEquals(SegmentFetcherFactory.getSegmentFetcher(HTTPS_PROTOCOL).getClass(), HttpsSegmentFetcher.class);
    assertEquals(SegmentFetcherFactory.getSegmentFetcher(PEER_2_PEER_PROTOCOL).getClass(), PeerServerSegmentFetcher.class);
  }

  @Test
  public void testPeerServerSegmentDownloadSuccess()
      throws Exception {
    SegmentFetcherFactory.fetchSegmentToLocal(new URI(StringUtil.join("/","server://", SEGMENT_1)),
        new File(System.getProperty("java.io.tmpdir"), "tmp-" + SEGMENT_1 + "." + System.currentTimeMillis()));

    FakeHttpSegmentFetcher httpSegmentFetcher = (FakeHttpSegmentFetcher) SegmentFetcherFactory.getSegmentFetcher(HTTP_PROTOCOL);
    assertEquals(httpSegmentFetcher._fetchFileToLocalCalled, 1);
  }

  @Test
  public void testPeerServerSegmentDownloadFailure()
      throws Exception {
    try {
      SegmentFetcherFactory.fetchSegmentToLocal(new URI(StringUtil.join("/", "server://", SEGMENT_2)),
          new File(System.getProperty("java.io.tmpdir"), "tmp-" + SEGMENT_2 + "." + System.currentTimeMillis()));
      fail("The segement download from peer servers should fail.");
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Operation failed"));
    }
  }

  public static class FakeHttpSegmentFetcher extends HttpSegmentFetcher {
    private int _fetchFileToLocalCalled = 0;

    @Override
    public void fetchSegmentToLocal(URI uri, File dest)
        throws Exception {
      // Verify that the correct server and its admin port is called.
      assertEquals(uri.toString(),
          StringUtil.join("/","http://s1:1008", "segments", TABLE_NAME_WITH_TYPE, SEGMENT_1));
      _fetchFileToLocalCalled++;
    }
  }
}
