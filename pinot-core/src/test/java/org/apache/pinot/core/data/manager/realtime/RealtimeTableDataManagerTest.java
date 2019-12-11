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
package org.apache.pinot.core.data.manager.realtime;

import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.data.manager.config.TableDataManagerConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;


public class RealtimeTableDataManagerTest {
  private static final String TABLE_NAME_WITH_TYPE = "testTable_REALTIME";
  public static final String SEGMENT_1 = "segment1";
  private RealtimeTableDataManager _dataManager;
  // Set once for the suite
  private File _tmpDir;
  String _clusterName = "dummyCluster";

  @BeforeClass
  public void setup()
      throws IOException {
    _tmpDir = File.createTempFile("OfflineTableDataManagerTest", null);
    _tmpDir.deleteOnExit();
  }

  @Test
  public void testDownloadSegmentFromPeerServers()
      throws Exception {
    TableDataManagerConfig config;
    {
      config = mock(TableDataManagerConfig.class);
      when(config.getTableName()).thenReturn(TABLE_NAME_WITH_TYPE);
      when(config.getDataDir()).thenReturn(_tmpDir.getAbsolutePath());
    }

    HelixAdmin helixAdmin;
    {
      ExternalView ev = new ExternalView(TABLE_NAME_WITH_TYPE);
      ev.setState(SEGMENT_1, "Server_localhost_1000", "ONLINE");
      ev.setState(SEGMENT_1, "Server_localhost_1001", "OFFLINE");
      helixAdmin = mock(HelixAdmin.class);
      when(helixAdmin.getResourceExternalView(_clusterName, TABLE_NAME_WITH_TYPE)).thenReturn(ev);
      when(helixAdmin.getConfigKeys(any(HelixConfigScope.class))).thenReturn(new ArrayList<>());
      Map<String, String> instanceConfigMap = new HashMap<>();
      instanceConfigMap.put(CommonConstants.Helix.Instance.ADMIN_PORT_KEY, "1008");
      when(helixAdmin.getConfig(any(HelixConfigScope.class), any(List.class))).thenReturn(instanceConfigMap);
    }


    _dataManager = new RealtimeTableDataManager(new Semaphore(1));

    _dataManager.init(config, "dummyInstance", mock(ZkHelixPropertyStore.class), new ServerMetrics(new MetricsRegistry()),
        helixAdmin, _clusterName);
    assertEquals(_dataManager.getPeerServerURI(SEGMENT_1),
        StringUtil.join("/","http://localhost:1008", "segments", TABLE_NAME_WITH_TYPE, SEGMENT_1));
  }
}
