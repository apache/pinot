/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.server.api.resources;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManagerProvider;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.server.starter.ServerInstance;
import com.linkedin.pinot.server.starter.helix.AdminApiApplication;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static org.mockito.Mockito.*;


public abstract class BaseResourceTest {
  private static final String AVRO_DATA_PATH = "data/test_data-mv.avro";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BaseResourceTest");
  protected static final String TABLE_NAME = "testTable";

  private final Map<String, TableDataManager> _tableDataManagerMap = new HashMap<>();
  protected final List<IndexSegment> _indexSegments = new ArrayList<>();

  private File _avroFile;
  private AdminApiApplication _adminApiApplication;
  protected WebTarget _webTarget;

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    Assert.assertTrue(INDEX_DIR.mkdirs());
    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA_PATH);
    Assert.assertNotNull(resourceUrl);
    _avroFile = new File(resourceUrl.getFile());

    // Mock the instance data manager
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(anyString())).thenAnswer(new Answer<TableDataManager>() {
      @SuppressWarnings("SuspiciousMethodCalls")
      @Override
      public TableDataManager answer(InvocationOnMock invocation) throws Throwable {
        return _tableDataManagerMap.get(invocation.getArguments()[0]);
      }
    });
    when(instanceDataManager.getTableDataManagers()).thenReturn(_tableDataManagerMap.values());

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
  public void tearDown() throws Exception {
    _adminApiApplication.stop();
    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }

    FileUtils.deleteQuietly(INDEX_DIR);
  }

  protected List<IndexSegment> setUpSegments(int numSegments) throws Exception {
    List<IndexSegment> indexSegments = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      indexSegments.add(setUpSegment(Integer.toString(_indexSegments.size())));
    }
    return indexSegments;
  }

  protected IndexSegment setUpSegment(String segmentNamePostfix) throws Exception {
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(_avroFile, INDEX_DIR, TABLE_NAME);
    config.setSegmentNamePostfix(segmentNamePostfix);
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
    IndexSegment indexSegment = ColumnarSegmentLoader.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.mmap);
    _indexSegments.add(indexSegment);
    _tableDataManagerMap.get(TABLE_NAME).addSegment(indexSegment);
    return indexSegment;
  }

  protected void addTable(String tableName) {
    TableDataManagerConfig tableDataManagerConfig = mock(TableDataManagerConfig.class);
    when(tableDataManagerConfig.getTableDataManagerType()).thenReturn("offline");
    when(tableDataManagerConfig.getTableName()).thenReturn(tableName);
    when(tableDataManagerConfig.getDataDir()).thenReturn(FileUtils.getTempDirectoryPath());
    @SuppressWarnings("unchecked")
    TableDataManager tableDataManager =
        TableDataManagerProvider.getTableDataManager(tableDataManagerConfig, "testInstance",
            mock(ZkHelixPropertyStore.class), mock(ServerMetrics.class));
    tableDataManager.start();
    _tableDataManagerMap.put(tableName, tableDataManager);
  }
}
