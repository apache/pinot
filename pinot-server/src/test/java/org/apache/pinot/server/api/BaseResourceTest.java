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

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.OfflineSegmentFetcherAndLoader;
import org.apache.pinot.core.data.manager.TableDataManager;
import org.apache.pinot.core.data.manager.config.TableDataManagerConfig;
import org.apache.pinot.core.data.manager.offline.OfflineTableDataManager;
import org.apache.pinot.core.data.manager.offline.SegmentCacheManager;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segments.v1.creator.SegmentTestUtils;
import org.apache.pinot.server.api.access.AllowAllAccessFactory;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.server.starter.helix.AdminApiApplication;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public abstract class BaseResourceTest {
  private static final String AVRO_DATA_PATH = "data/test_data-mv.avro";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BaseResourceTest");
  protected static final String TABLE_NAME = "testTable";

  private final Map<String, TableDataManager> _tableDataManagerMap = new HashMap<>();
  protected final List<ImmutableSegment> _realtimeIndexSegments = new ArrayList<>();
  protected final List<ImmutableSegment> _offlineIndexSegments = new ArrayList<>();
  private File _avroFile;
  private AdminApiApplication _adminApiApplication;
  protected WebTarget _webTarget;

  @SuppressWarnings("SuspiciousMethodCalls")
  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    Assert.assertTrue(INDEX_DIR.mkdirs());
    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA_PATH);
    Assert.assertNotNull(resourceUrl);
    _avroFile = new File(resourceUrl.getFile());

    // Mock the instance data manager
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(anyString()))
        .thenAnswer(invocation -> _tableDataManagerMap.get(invocation.getArguments()[0]));
    when(instanceDataManager.getAllTables()).thenReturn(_tableDataManagerMap.keySet());

    // Mock the server instance
    ServerInstance serverInstance = mock(ServerInstance.class);
    when(serverInstance.getInstanceDataManager()).thenReturn(instanceDataManager);
    when(serverInstance.getInstanceDataManager().getSegmentFileDirectory())
        .thenReturn(FileUtils.getTempDirectoryPath());
    // Add the default tables and segments.
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);

    addTable(realtimeTableName);
    addTable(offlineTableName);
    setUpSegment(realtimeTableName, "default", _realtimeIndexSegments);
    setUpSegment(offlineTableName, "default", _offlineIndexSegments);

    _adminApiApplication = new AdminApiApplication(serverInstance, new AllowAllAccessFactory());
    _adminApiApplication.start(CommonConstants.Server.DEFAULT_ADMIN_API_PORT);
    _webTarget = ClientBuilder.newClient().target(_adminApiApplication.getBaseUri());
  }

  @AfterClass
  public void tearDown() {
    _adminApiApplication.stop();
    for (ImmutableSegment immutableSegment : _realtimeIndexSegments) {
      immutableSegment.destroy();
    }
    for (ImmutableSegment immutableSegment : _offlineIndexSegments) {
      immutableSegment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  protected List<ImmutableSegment> setUpSegments(String tableNameWithType, int numSegments,
      List<ImmutableSegment> segments)
      throws Exception {
    List<ImmutableSegment> immutableSegments = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      immutableSegments.add(setUpSegment(tableNameWithType, Integer.toString(_realtimeIndexSegments.size()), segments));
    }
    return immutableSegments;
  }

  protected ImmutableSegment setUpSegment(String tableNameWithType, String segmentNamePostfix,
      List<ImmutableSegment> segments)
      throws Exception {
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(_avroFile, INDEX_DIR, tableNameWithType);
    config.setSegmentNamePostfix(segmentNamePostfix);
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
    ImmutableSegment immutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.mmap);
    segments.add(immutableSegment);
    _tableDataManagerMap.get(tableNameWithType).addSegment(immutableSegment);
    return immutableSegment;
  }

  @SuppressWarnings("unchecked")
  protected void addTable(String tableNameWithType) {
    TableDataManagerConfig tableDataManagerConfig = mock(TableDataManagerConfig.class);
    when(tableDataManagerConfig.getTableName()).thenReturn(tableNameWithType);
    when(tableDataManagerConfig.getDataDir()).thenReturn(INDEX_DIR.getAbsolutePath());
    // NOTE: Use OfflineTableDataManager for both OFFLINE and REALTIME table because RealtimeTableDataManager requires
    //       table config.
    TableDataManager tableDataManager =
        new OfflineTableDataManager(mock(OfflineSegmentFetcherAndLoader.class), mock(SegmentCacheManager.class));
    tableDataManager
        .init(tableDataManagerConfig, "testInstance", mock(ZkHelixPropertyStore.class), mock(ServerMetrics.class),
            mock(HelixManager.class));
    tableDataManager.start();
    _tableDataManagerMap.put(tableNameWithType, tableDataManager);
  }
}
