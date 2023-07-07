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
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.OfflineTableDataManager;
import org.apache.pinot.core.data.manager.realtime.SegmentUploader;
import org.apache.pinot.core.transport.HttpServerThreadPoolConfig;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.segment.local.data.manager.TableDataManagerParams;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.server.access.AllowAllAccessFactory;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public abstract class BaseResourceTest {
  private static final String AVRO_DATA_PATH = "data/test_data-mv.avro";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BaseResourceTest");
  protected static final String TABLE_NAME = "testTable";
  protected static final String LLC_SEGMENT_NAME_FOR_UPLOAD_SUCCESS =
      new LLCSegmentName(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME), 1, 0, System.currentTimeMillis())
          .getSegmentName();
  protected static final String LLC_SEGMENT_NAME_FOR_UPLOAD_FAILURE =
      new LLCSegmentName(TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME), 2, 0, System.currentTimeMillis())
          .getSegmentName();
  protected static final String SEGMENT_DOWNLOAD_URL = StringUtil
      .join("/", "hdfs://root", TABLE_NAME, LLC_SEGMENT_NAME_FOR_UPLOAD_SUCCESS);

  private final Map<String, TableDataManager> _tableDataManagerMap = new HashMap<>();
  protected final List<ImmutableSegment> _realtimeIndexSegments = new ArrayList<>();
  protected final List<ImmutableSegment> _offlineIndexSegments = new ArrayList<>();
  protected File _avroFile;
  protected AdminApiApplication _adminApiApplication;
  protected WebTarget _webTarget;
  protected String _instanceId;

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
    when(serverInstance.getServerMetrics()).thenReturn(mock(ServerMetrics.class));
    when(serverInstance.getInstanceDataManager()).thenReturn(instanceDataManager);
    when(serverInstance.getInstanceDataManager().getSegmentFileDirectory())
        .thenReturn(FileUtils.getTempDirectoryPath());
    when(serverInstance.getHelixManager()).thenReturn(mock(HelixManager.class));

    // Mock the segment uploader
    SegmentUploader segmentUploader = mock(SegmentUploader.class);
    when(segmentUploader.uploadSegment(any(File.class), eq(new LLCSegmentName(LLC_SEGMENT_NAME_FOR_UPLOAD_SUCCESS))))
        .thenReturn(new URI(SEGMENT_DOWNLOAD_URL));
    when(segmentUploader.uploadSegment(any(File.class), eq(new LLCSegmentName(LLC_SEGMENT_NAME_FOR_UPLOAD_FAILURE))))
        .thenReturn(null);
    when(instanceDataManager.getSegmentUploader()).thenReturn(segmentUploader);

    // Add the default tables and segments.
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);

    addTable(realtimeTableName);
    addTable(offlineTableName);
    setUpSegment(realtimeTableName, null, "default", _realtimeIndexSegments);
    setUpSegment(offlineTableName, null, "default", _offlineIndexSegments);

    PinotConfiguration serverConf = new PinotConfiguration();
    String hostname = serverConf.getProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST,
        serverConf.getProperty(CommonConstants.Helix.SET_INSTANCE_ID_TO_HOSTNAME_KEY, false)
            ? NetUtils.getHostnameOrAddress() : NetUtils.getHostAddress());
    int port = serverConf.getProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT,
        CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT);
    _instanceId = CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + hostname + "_" + port;
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_ID, _instanceId);
    _adminApiApplication = new AdminApiApplication(serverInstance, new AllowAllAccessFactory(), serverConf);
    _adminApiApplication.start(Collections.singletonList(
        new ListenerConfig(CommonConstants.HTTP_PROTOCOL, "0.0.0.0", CommonConstants.Server.DEFAULT_ADMIN_API_PORT,
            CommonConstants.HTTP_PROTOCOL, new TlsConfig(), HttpServerThreadPoolConfig.defaultInstance())));

    _webTarget = ClientBuilder.newClient().target(
        String.format("http://%s:%d", NetUtils.getHostAddress(), CommonConstants.Server.DEFAULT_ADMIN_API_PORT));
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
      immutableSegments
          .add(setUpSegment(tableNameWithType, null, Integer.toString(_realtimeIndexSegments.size()), segments));
    }
    return immutableSegments;
  }

  protected ImmutableSegment setUpSegment(String tableNameWithType, String segmentName, String segmentNamePostfix,
      List<ImmutableSegment> segments)
      throws Exception {
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(_avroFile, INDEX_DIR, tableNameWithType);
    config.setSegmentName(segmentName);
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
    TableDataManager tableDataManager = new OfflineTableDataManager();
    tableDataManager
        .init(tableDataManagerConfig, "testInstance", mock(ZkHelixPropertyStore.class), mock(ServerMetrics.class),
            mock(HelixManager.class), null, new TableDataManagerParams(0, false, -1));
    tableDataManager.start();
    _tableDataManagerMap.put(tableNameWithType, tableDataManager);
  }
}
