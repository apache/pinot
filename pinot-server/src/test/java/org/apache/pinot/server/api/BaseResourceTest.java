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
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.TableDataManager;
import org.apache.pinot.core.data.manager.config.TableDataManagerConfig;
import org.apache.pinot.core.data.manager.offline.TableDataManagerProvider;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segments.v1.creator.SegmentTestUtils;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.server.starter.helix.AdminApiApplication;
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
  protected final List<ImmutableSegment> _indexSegments = new ArrayList<>();

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

    // Add the default table and segment
    addTable(TABLE_NAME);
    setUpSegment("default");

    _adminApiApplication = new AdminApiApplication(serverInstance);
    _adminApiApplication.start(CommonConstants.Server.DEFAULT_ADMIN_API_PORT);
    _webTarget = ClientBuilder.newClient().target(_adminApiApplication.getBaseUri());
  }

  @AfterClass
  public void tearDown() {
    _adminApiApplication.stop();
    for (ImmutableSegment immutableSegment : _indexSegments) {
      immutableSegment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  protected List<ImmutableSegment> setUpSegments(int numSegments)
      throws Exception {
    List<ImmutableSegment> immutableSegments = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      immutableSegments.add(setUpSegment(Integer.toString(_indexSegments.size())));
    }
    return immutableSegments;
  }

  protected ImmutableSegment setUpSegment(String segmentNamePostfix)
      throws Exception {
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(_avroFile, INDEX_DIR, TABLE_NAME);
    config.setSegmentNamePostfix(segmentNamePostfix);
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
    ImmutableSegment immutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.mmap);
    _indexSegments.add(immutableSegment);
    _tableDataManagerMap.get(TABLE_NAME).addSegment(immutableSegment);
    return immutableSegment;
  }

  @SuppressWarnings("unchecked")
  protected void addTable(String tableName) {
    TableDataManagerConfig tableDataManagerConfig = mock(TableDataManagerConfig.class);
    when(tableDataManagerConfig.getTableDataManagerType()).thenReturn("OFFLINE");
    when(tableDataManagerConfig.getTableName()).thenReturn(tableName);
    when(tableDataManagerConfig.getDataDir()).thenReturn(FileUtils.getTempDirectoryPath());
    TableDataManager tableDataManager = TableDataManagerProvider
        .getTableDataManager(tableDataManagerConfig, "testInstance", mock(ZkHelixPropertyStore.class),
            mock(ServerMetrics.class));
    tableDataManager.start();
    _tableDataManagerMap.put(tableName, tableDataManager);
  }
}
