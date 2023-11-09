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
import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.apache.pinot.common.utils.grpc.GrpcRequestBuilder;
import org.apache.pinot.core.query.reduce.DataTableReducer;
import org.apache.pinot.core.query.reduce.DataTableReducerContext;
import org.apache.pinot.core.query.reduce.ResultReducerFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;


public class OfflineGRPCServerIntegrationTest extends BaseClusterIntegrationTest {
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(2);
  private static final DataTableReducerContext DATATABLE_REDUCER_CONTEXT = new DataTableReducerContext(
      EXECUTOR_SERVICE, 2, 10000, 10000, 5000);

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  public GrpcQueryClient getGrpcQueryClient() {
    return new GrpcQueryClient("localhost", getServerGrpcPort());
  }

  @Test
  public void testGrpcQueryServer()
      throws Exception {
    GrpcQueryClient queryClient = getGrpcQueryClient();
    String sql = "SELECT * FROM mytable_OFFLINE LIMIT 1000000 OPTION(timeoutMs=30000)";
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest(sql);
    List<String> segments = _helixResourceManager.getSegmentsFor("mytable_OFFLINE", false);

    GrpcRequestBuilder requestBuilder = new GrpcRequestBuilder().setSegments(segments);
    testNonStreamingRequest(queryClient.submit(requestBuilder.setSql(sql).build()));
    testNonStreamingRequest(queryClient.submit(requestBuilder.setBrokerRequest(brokerRequest).build()));

    requestBuilder.setEnableStreaming(true);
    testStreamingRequest(queryClient.submit(requestBuilder.setSql(sql).build()));
    testStreamingRequest(queryClient.submit(requestBuilder.setBrokerRequest(brokerRequest).build()));
    queryClient.close();
  }

  @Test(dataProvider = "provideSqlTestCases")
  public void testQueryingGrpcServer(String sql)
      throws Exception {
    GrpcQueryClient queryClient = getGrpcQueryClient();
    List<String> segments = _helixResourceManager.getSegmentsFor("mytable_OFFLINE", false);

    GrpcRequestBuilder requestBuilder = new GrpcRequestBuilder().setSegments(segments);
    DataTable dataTable = collectNonStreamingRequestResult(queryClient.submit(requestBuilder.setSql(sql).build()));

    requestBuilder.setEnableStreaming(true);
    collectAndCompareResult(sql, queryClient.submit(requestBuilder.setSql(sql).build()), dataTable);
    queryClient.close();
  }

  @DataProvider(name = "provideSqlTestCases")
  public Object[][] provideSqlAndResultRowsAndNumDocScanTestCases() {
    List<Object[]> entries = new ArrayList<>();

    // select only
    entries.add(new Object[]{"SELECT * FROM mytable_OFFLINE LIMIT 10000000"});
    entries.add(new Object[]{"SELECT * FROM mytable_OFFLINE WHERE DaysSinceEpoch > 16312 LIMIT 10000000"});
    entries.add(new Object[]{
        "SELECT timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable_OFFLINE LIMIT 10000000"
    });

    // select only with early termination
    entries.add(new Object[]{"SELECT * FROM mytable_OFFLINE LIMIT 10"});

    // aggregate
    entries.add(new Object[]{"SELECT count(*) FROM mytable_OFFLINE"});
    entries.add(new Object[]{"SELECT SUM(ArrTime) FROM mytable_OFFLINE"});

    // group-by
    entries.add(new Object[]{"SELECT count(*) FROM mytable_OFFLINE GROUP BY arrayLength(DivAirports)"});
    entries.add(new Object[]{"SELECT DISTINCTCOUNT(AirlineID) FROM mytable_OFFLINE GROUP BY Carrier"});
    entries.add(new Object[]{"SELECT count(*), sum(ArrTime) FROM mytable_OFFLINE GROUP BY AirlineID"});

    // distinct
    entries.add(new Object[]{"SELECT DISTINCT(AirlineID) FROM mytable_OFFLINE LIMIT 1000000"});
    entries.add(new Object[]{"SELECT AirlineID, ArrTime FROM mytable_OFFLINE "
        + "GROUP BY AirlineID, ArrTime LIMIT 1000000"});

    // order by
    entries.add(new Object[]{"SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') "
        + "FROM mytable_OFFLINE ORDER BY DaysSinceEpoch limit 1000000"});

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
        reducer.reduceAndSetResults("mytable_OFFLINE", cachedDataSchema, dataTableMap, streamingBrokerResponse,
            DATATABLE_REDUCER_CONTEXT, mock(BrokerMetrics.class));
        dataTableMap.clear();
        dataTableMap.put(mock(ServerRoutingInstance.class), nonStreamResultDataTable);
        BrokerResponseNative nonStreamBrokerResponse = new BrokerResponseNative();
        reducer.reduceAndSetResults("mytable_OFFLINE", cachedDataSchema, dataTableMap, nonStreamBrokerResponse,
            DATATABLE_REDUCER_CONTEXT, mock(BrokerMetrics.class));
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
        assertTrue(metadata.size() == 1 && metadata.containsKey(MetadataKey.RESPONSE_SER_CPU_TIME_NS.getName()));
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

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(getTableName());

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
