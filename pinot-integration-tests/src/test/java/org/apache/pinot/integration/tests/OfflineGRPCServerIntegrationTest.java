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
package org.apache.pinot.integration.tests;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.grpc.ServerGrpcQueryClient;
import org.apache.pinot.common.utils.grpc.ServerGrpcRequestBuilder;
import org.apache.pinot.core.accounting.ResourceUsageAccountantFactory;
import org.apache.pinot.core.query.reduce.DataTableReducer;
import org.apache.pinot.core.query.reduce.DataTableReducerContext;
import org.apache.pinot.core.query.reduce.ResultReducerFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;


public class OfflineGRPCServerIntegrationTest extends SharedRichClusterIntegrationTest {
  private static final String SHARED_TABLE_NAME = "offline_grpc_server";

  private static final ExecutorService EXECUTOR_SERVICE =
      QueryThreadContext.contextAwareExecutorService(Executors.newFixedThreadPool(2));
  private static final DataTableReducerContext DATATABLE_REDUCER_CONTEXT =
      new DataTableReducerContext(EXECUTOR_SERVICE, 2, 10000, 10000, 5000, 128);

  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    _classSegmentDir = getClassSegmentDir();
    _classTarDir = getClassTarDir();
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    cleanTableAndSchema();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_classTempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _classSegmentDir,
        _classTarDir);
    uploadSegments(getTableName(), _classTarDir);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
    waitForGrpcServerSegmentsLoaded(600_000L);
  }

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME : super.getTableName();
  }

  @Override
  protected int getNumReplicas() {
    if (isSharedRichClusterEnabled()) {
      return Math.max(super.getNumReplicas(), _serverStarters.size());
    }
    return super.getNumReplicas();
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    super.overrideBrokerConf(brokerConf);

    // Enable thread CPU/memory tracking but not killing queries
    brokerConf.setProperty(Broker.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    brokerConf.setProperty(Broker.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
    String prefix = Accounting.BROKER_PREFIX + ".";
    brokerConf.setProperty(prefix + Accounting.Keys.FACTORY_NAME, ResourceUsageAccountantFactory.class.getName());
    brokerConf.setProperty(prefix + Accounting.Keys.ENABLE_THREAD_CPU_SAMPLING, true);
    brokerConf.setProperty(prefix + Accounting.Keys.ENABLE_THREAD_MEMORY_SAMPLING, true);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    super.overrideServerConf(serverConf);

    // Enable thread CPU/memory tracking but not killing queries
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
    String prefix = Accounting.SERVER_PREFIX + ".";
    serverConf.setProperty(prefix + Accounting.Keys.FACTORY_NAME, ResourceUsageAccountantFactory.class.getName());
    serverConf.setProperty(prefix + Accounting.Keys.ENABLE_THREAD_CPU_SAMPLING, true);
    serverConf.setProperty(prefix + Accounting.Keys.ENABLE_THREAD_MEMORY_SAMPLING, true);
  }

  public ServerGrpcQueryClient getGrpcQueryClient() {
    return new ServerGrpcQueryClient("localhost", getServerGrpcPort());
  }

  @Test
  public void testGrpcQueryServer()
      throws Exception {
    try (ServerGrpcQueryClient queryClient = getGrpcQueryClient()) {
      String sql = "SELECT * FROM " + getOfflineTableName() + " LIMIT 1000000 OPTION(timeoutMs=30000)";
      BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest(sql);
      List<String> segments = _helixResourceManager.getSegmentsFor(getOfflineTableName(), true);

      ServerGrpcRequestBuilder requestBuilder = new ServerGrpcRequestBuilder().setSegments(segments);
      testNonStreamingRequest(queryClient.submit(requestBuilder.setSql(sql).build()));
      testNonStreamingRequest(queryClient.submit(requestBuilder.setBrokerRequest(brokerRequest).build()));

      requestBuilder.setEnableStreaming(true);
      testStreamingRequest(queryClient.submit(requestBuilder.setSql(sql).build()));
      testStreamingRequest(queryClient.submit(requestBuilder.setBrokerRequest(brokerRequest).build()));
    }
  }

  @Test(dataProvider = "provideSqlTestCases")
  public void testQueryingGrpcServer(String sql)
      throws Exception {
    try (ServerGrpcQueryClient queryClient = getGrpcQueryClient()) {
      List<String> segments = _helixResourceManager.getSegmentsFor(getOfflineTableName(), true);
      ServerGrpcRequestBuilder requestBuilder = new ServerGrpcRequestBuilder().setSql(sql).setSegments(segments);
      DataTable dataTable = collectNonStreamingRequestResult(queryClient.submit(requestBuilder.build()));
      collectAndCompareResult(sql, queryClient.submit(requestBuilder.setEnableStreaming(true).build()), dataTable);
    }
  }

  @DataProvider(name = "provideSqlTestCases")
  public Object[][] provideSqlAndResultRowsAndNumDocScanTestCases() {
    List<Object[]> entries = new ArrayList<>();
    String offlineTableName = getOfflineTableName();

    // select only
    entries.add(new Object[]{"SELECT * FROM " + offlineTableName + " LIMIT 10000000"});
    entries.add(new Object[]{"SELECT * FROM " + offlineTableName + " WHERE DaysSinceEpoch > 16312 LIMIT 10000000"});
    entries.add(new Object[]{
        "SELECT timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM " + offlineTableName + " LIMIT 10000000"
    });

    // select only with early termination
    entries.add(new Object[]{"SELECT * FROM " + offlineTableName + " LIMIT 10"});

    // aggregate
    entries.add(new Object[]{"SELECT count(*) FROM " + offlineTableName});
    entries.add(new Object[]{"SELECT SUM(ArrTime) FROM " + offlineTableName});

    // group-by
    entries.add(new Object[]{"SELECT count(*) FROM " + offlineTableName + " GROUP BY arrayLength(DivAirports)"});
    entries.add(new Object[]{"SELECT DISTINCTCOUNT(AirlineID) FROM " + offlineTableName + " GROUP BY Carrier"});
    entries.add(new Object[]{"SELECT count(*), sum(ArrTime) FROM " + offlineTableName + " GROUP BY AirlineID"});

    // distinct
    entries.add(new Object[]{"SELECT DISTINCT(AirlineID) FROM " + offlineTableName + " LIMIT 1000000"});
    entries.add(new Object[]{
        "SELECT AirlineID, ArrTime FROM " + offlineTableName + " GROUP BY AirlineID, ArrTime LIMIT 1000000"
    });

    // order by
    entries.add(new Object[]{
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM " + offlineTableName + " "
            + "ORDER BY DaysSinceEpoch limit 1000000"
    });

    return entries.toArray(new Object[entries.size()][]);
  }

  private DataTable collectNonStreamingRequestResult(Iterator<Server.ServerResponse> nonStreamingResponses)
      throws Exception {
    assertTrue(nonStreamingResponses.hasNext());
    Server.ServerResponse nonStreamingResponse = nonStreamingResponses.next();
    assertEquals(nonStreamingResponse.getMetadataMap().get(CommonConstants.Query.Response.MetadataKeys.RESPONSE_TYPE),
        CommonConstants.Query.Response.ResponseType.NON_STREAMING);
    DataTable dataTable = DataTableFactory.getDataTable(nonStreamingResponse.getPayload().asReadOnlyByteBuffer());
    assertNotNull(dataTable.getDataSchema());
    return dataTable;
  }

  private void collectAndCompareResult(String sql, Iterator<Server.ServerResponse> streamingResponses,
      DataTable nonStreamResultDataTable)
      throws Exception {
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    DataSchema cachedDataSchema = null;
    while (streamingResponses.hasNext()) {
      Server.ServerResponse streamingResponse = streamingResponses.next();
      DataTable dataTable = DataTableFactory.getDataTable(streamingResponse.getPayload().asReadOnlyByteBuffer());
      String responseType =
          streamingResponse.getMetadataMap().get(CommonConstants.Query.Response.MetadataKeys.RESPONSE_TYPE);
      if (responseType.equals(CommonConstants.Query.Response.ResponseType.DATA)) {
        // verify the returned data table metadata contains "responseSerializationCpuTimeNs".
        // this is true for both selection-only streaming and full processed streaming results.
        Map<String, String> metadata = dataTable.getMetadata();
        assertTrue(metadata.containsKey(DataTable.MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName()));
        assertNotNull(dataTable.getDataSchema());
        cachedDataSchema = dataTable.getDataSchema();
        // adding them to a fake dataTableMap for reduce
        dataTableMap.put(mock(ServerRoutingInstance.class), dataTable);
      } else {
        // compare result dataTable against nonStreamingResultDataTable
        // Process server response.
        QueryContext queryContext = QueryContextConverterUtils.getQueryContext(sql);
        DataTableReducer reducer = ResultReducerFactory.getResultReducer(queryContext);
        BrokerResponseNative streamingBrokerResponse = new BrokerResponseNative();
        try (QueryThreadContext ignore = useMultiStageQueryEngine() ? QueryThreadContext.openForMseTest()
            : QueryThreadContext.openForSseTest()) {
          reducer.reduceAndSetResults(getOfflineTableName(), cachedDataSchema, dataTableMap, streamingBrokerResponse,
              DATATABLE_REDUCER_CONTEXT, mock(BrokerMetrics.class));
        }
        BrokerResponseNative nonStreamBrokerResponse = new BrokerResponseNative();
        try (QueryThreadContext ignore = useMultiStageQueryEngine() ? QueryThreadContext.openForMseTest()
            : QueryThreadContext.openForSseTest()) {
          reducer.reduceAndSetResults(getOfflineTableName(), nonStreamResultDataTable.getDataSchema(),
              Map.of(mock(ServerRoutingInstance.class), nonStreamResultDataTable), nonStreamBrokerResponse,
              DATATABLE_REDUCER_CONTEXT, mock(BrokerMetrics.class));
        }
        assertEquals(streamingBrokerResponse.getResultTable().getRows().size(),
            nonStreamBrokerResponse.getResultTable().getRows().size());

        // validate final metadata return
        assertEquals(responseType, CommonConstants.Query.Response.ResponseType.METADATA);
        assertFalse(streamingResponses.hasNext());
        assertNull(dataTable.getDataSchema());
        assertEquals(dataTable.getNumberOfRows(), 0);
        assertEquals(dataTable.getMetadata().get(DataTable.MetadataKey.NUM_DOCS_SCANNED.getName()),
            nonStreamResultDataTable.getMetadata().get(DataTable.MetadataKey.NUM_DOCS_SCANNED.getName()));
      }
    }
  }

  private void testNonStreamingRequest(Iterator<Server.ServerResponse> nonStreamingResponses)
      throws Exception {
    int expectedNumDocs = (int) getCountStarResult();
    DataTable dataTable = collectNonStreamingRequestResult(nonStreamingResponses);
    assertEquals(dataTable.getNumberOfRows(), expectedNumDocs);
    Map<String, String> metadata = dataTable.getMetadata();
    assertEquals(metadata.get(MetadataKey.NUM_DOCS_SCANNED.getName()), Integer.toString(expectedNumDocs));
  }

  private void testStreamingRequest(Iterator<Server.ServerResponse> streamingResponses)
      throws Exception {
    int expectedNumDocs = (int) getCountStarResult();
    int numTotalDocs = 0;
    while (streamingResponses.hasNext()) {
      Server.ServerResponse streamingResponse = streamingResponses.next();
      DataTable dataTable = DataTableFactory.getDataTable(streamingResponse.getPayload().asReadOnlyByteBuffer());
      String responseType =
          streamingResponse.getMetadataMap().get(CommonConstants.Query.Response.MetadataKeys.RESPONSE_TYPE);
      if (responseType.equals(CommonConstants.Query.Response.ResponseType.DATA)) {
        // verify the returned data table metadata only contains "responseSerializationCpuTimeNs".
        Map<String, String> metadata = dataTable.getMetadata();
        assertTrue(metadata.containsKey(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName()));
        assertNotNull(dataTable.getDataSchema());
        numTotalDocs += dataTable.getNumberOfRows();
      } else {
        assertEquals(responseType, CommonConstants.Query.Response.ResponseType.METADATA);
        assertFalse(streamingResponses.hasNext());
        assertEquals(numTotalDocs, expectedNumDocs);
        assertNull(dataTable.getDataSchema());
        assertEquals(dataTable.getNumberOfRows(), 0);
        Map<String, String> metadata = dataTable.getMetadata();
        assertEquals(metadata.get(MetadataKey.NUM_DOCS_SCANNED.getName()), Integer.toString(expectedNumDocs));
      }
    }
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanTableAndSchema);
    exception = runCleanup(exception, this::closeH2Connection);
    exception = runCleanup(exception, this::closePinotConnections);
    exception = runCleanup(exception, this::stopServer);
    exception = runCleanup(exception, this::stopBroker);
    exception = runCleanup(exception, this::stopControllerIfStarted);
    exception = runCleanup(exception, this::stopZk);
    exception = runCleanup(exception, this::cleanTempDirectory);
    if (exception != null) {
      throw exception;
    }
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled() ? new File(_tempDir, "testData") : _tempDir;
  }

  private File getClassSegmentDir() {
    return isSharedRichClusterEnabled() ? new File(_classTempDir, "segmentDir") : _segmentDir;
  }

  private File getClassTarDir() {
    return isSharedRichClusterEnabled() ? new File(_classTempDir, "tarDir") : _tarDir;
  }

  private String getOfflineTableName() {
    return TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
  }

  private void waitForGrpcServerSegmentsLoaded(long timeoutMs) {
    if (_serverStarters.isEmpty()) {
      return;
    }

    String offlineTableName = getOfflineTableName();
    String serverInstance = _serverStarters.get(0).getInstanceId();
    List<String> expectedSegments = _helixResourceManager.getSegmentsFor(offlineTableName, true);
    TestUtils.waitForCondition(aVoid -> {
      Map<String, List<String>> onlineSegmentsMap =
          _helixResourceManager.getServerToOnlineSegmentsMapFromEV(offlineTableName, true);
      List<String> onlineSegments = onlineSegmentsMap.get(serverInstance);
      return onlineSegments != null && onlineSegments.containsAll(expectedSegments);
    }, timeoutMs, "Failed to load all segments for table: " + offlineTableName + " on server: " + serverInstance);
  }

  private void cleanTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String tableName = getTableName();
    String offlineTableName = getOfflineTableName();
    if (_helixResourceManager.getAllTables().contains(offlineTableName) || _helixResourceManager.hasOfflineTable(
        tableName)) {
      dropOfflineTable(tableName);
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }
    if (_helixResourceManager.getSchema(tableName) != null) {
      deleteSchema(tableName);
    }
  }

  private void closeH2Connection()
      throws Exception {
    if (_h2Connection != null) {
      _h2Connection.close();
      _h2Connection = null;
    }
  }

  private void closePinotConnections() {
    if (_pinotConnection != null) {
      _pinotConnection.close();
      _pinotConnection = null;
    }
    if (_pinotConnectionV2 != null) {
      _pinotConnectionV2.close();
      _pinotConnectionV2 = null;
    }
  }

  private void stopControllerIfStarted() {
    if (_controllerStarter != null) {
      stopController();
    }
  }

  private void cleanTempDirectory()
      throws Exception {
    if (_classTempDir != null) {
      FileUtils.deleteDirectory(_classTempDir);
    }
  }

  private Exception runCleanup(Exception firstException, Cleanup cleanup) {
    try {
      cleanup.run();
    } catch (Exception e) {
      if (firstException == null) {
        return e;
      }
      firstException.addSuppressed(e);
    }
    return firstException;
  }

  private interface Cleanup {
    void run()
        throws Exception;
  }
}
