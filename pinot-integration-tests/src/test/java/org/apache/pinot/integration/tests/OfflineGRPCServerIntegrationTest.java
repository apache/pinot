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

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.common.utils.DataTable.MetadataKey;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.apache.pinot.common.utils.grpc.GrpcRequestBuilder;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class OfflineGRPCServerIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 1;
  private static final int NUM_SEGMENTS = 12;

  private final List<ServiceStatus.ServiceStatusCallback> _serviceStatusCallbacks =
      new ArrayList<>(getNumBrokers() + getNumServers());
  private String _schemaFileName = DEFAULT_SCHEMA_FILE_NAME;
  // Cache the table size after removing an index via reloading. Once this value
  // is set, assert that table size always gets back to this value after removing
  // any other kind of index.
  private long _tableSizeAfterRemovingIndex;

  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  protected int getNumServers() {
    return NUM_SERVERS;
  }

  @Override
  protected String getSchemaFileName() {
    return _schemaFileName;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(getNumBrokers());
    startServers();

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

    // Set up service status callbacks
    // NOTE: put this step after creating the table and uploading all segments so that brokers and servers can find the
    // resources to monitor
    registerCallbackHandlers();

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  protected void startServers() {
    // Enable gRPC server
    PinotConfiguration serverConfig = getDefaultServerConfiguration();
    serverConfig.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_GRPC_SERVER, true);
    startServer(serverConfig);
  }

  private void registerCallbackHandlers() {
    List<String> instances = _helixAdmin.getInstancesInCluster(getHelixClusterName());
    instances.removeIf(
        instance -> (!instance.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE) && !instance.startsWith(
            CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)));
    List<String> resourcesInCluster = _helixAdmin.getResourcesInCluster(getHelixClusterName());
    resourcesInCluster.removeIf(resource -> (!TableNameBuilder.isTableResource(resource)
        && !CommonConstants.Helix.BROKER_RESOURCE_INSTANCE.equals(resource)));
    for (String instance : instances) {
      List<String> resourcesToMonitor = new ArrayList<>();
      for (String resourceName : resourcesInCluster) {
        IdealState idealState = _helixAdmin.getResourceIdealState(getHelixClusterName(), resourceName);
        for (String partitionName : idealState.getPartitionSet()) {
          if (idealState.getInstanceSet(partitionName).contains(instance)) {
            resourcesToMonitor.add(resourceName);
            break;
          }
        }
      }
      _serviceStatusCallbacks.add(new ServiceStatus.MultipleCallbackServiceStatusCallback(ImmutableList.of(
          new ServiceStatus.IdealStateAndCurrentStateMatchServiceStatusCallback(_helixManager, getHelixClusterName(),
              instance, resourcesToMonitor, 100.0),
          new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_helixManager, getHelixClusterName(),
              instance, resourcesToMonitor, 100.0))));
    }
  }

  @Test
  public void testGrpcQueryServer()
      throws Exception {
    GrpcQueryClient queryClient = new GrpcQueryClient("localhost", CommonConstants.Server.DEFAULT_GRPC_PORT);
    String sql = "SELECT * FROM mytable_OFFLINE LIMIT 1000000";
    BrokerRequest brokerRequest = new Pql2Compiler().compileToBrokerRequest(sql);
    List<String> segments = _helixResourceManager.getSegmentsFor("mytable_OFFLINE");

    GrpcRequestBuilder requestBuilder = new GrpcRequestBuilder().setSegments(segments);
    testNonStreamingRequest(queryClient.submit(requestBuilder.setSql(sql).build()));
    testNonStreamingRequest(queryClient.submit(requestBuilder.setBrokerRequest(brokerRequest).build()));

    requestBuilder.setEnableStreaming(true);
    testStreamingRequest(queryClient.submit(requestBuilder.setSql(sql).build()));
    testStreamingRequest(queryClient.submit(requestBuilder.setBrokerRequest(brokerRequest).build()));
  }

  @Test(dataProvider = "provideSqlTestCases")
  public void testQueryingGrpcServer(String sql)
      throws Exception {
    GrpcQueryClient queryClient = new GrpcQueryClient("localhost", CommonConstants.Server.DEFAULT_GRPC_PORT);
    List<String> segments = _helixResourceManager.getSegmentsFor("mytable_OFFLINE");

    GrpcRequestBuilder requestBuilder = new GrpcRequestBuilder().setSegments(segments);
    DataTable dataTable = collectNonStreamingRequestResult(queryClient.submit(requestBuilder.setSql(sql).build()));

    requestBuilder.setEnableStreaming(true);
    collectAndCompareResult(queryClient.submit(requestBuilder.setSql(sql).build()), dataTable);
  }

  @DataProvider(name = "provideSqlTestCases")
  public Object[][] provideSqlAndResultRowsAndNumDocScanTestCases() {
    List<Object[]> entries = new ArrayList<>();

    // select only
    entries.add(new Object[]{"SELECT * FROM mytable_OFFLINE LIMIT 10000000"});
    entries.add(new Object[]{"SELECT * FROM mytable_OFFLINE WHERE DaysSinceEpoch > 16312 LIMIT 10000000"});
    entries.add(new Object[]{
        "SELECT timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable_OFFLINE LIMIT 10000000"});

    // aggregate
    entries.add(new Object[]{"SELECT count(*) FROM mytable_OFFLINE"});
    entries.add(new Object[]{"SELECT count(*) FROM mytable_OFFLINE GROUP BY arrayLength(DivAirports)"});

    // distinct count
    entries.add(new Object[]{"SELECT DISTINCTCOUNT(AirlineID) FROM mytable_OFFLINE GROUP BY Carrier"});

    // order by
    entries.add(new Object[]{"SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') "
        + "FROM mytable_OFFLINE ORDER BY DaysSinceEpoch limit 10000"});

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

  private void collectAndCompareResult(Iterator<Server.ServerResponse> streamingResponses,
      DataTable nonStreamResultDataTable)
      throws Exception {
    int numTotalDocs = 0;
    while (streamingResponses.hasNext()) {
      Server.ServerResponse streamingResponse = streamingResponses.next();
      DataTable dataTable = DataTableFactory.getDataTable(streamingResponse.getPayload().asReadOnlyByteBuffer());
      String responseType =
          streamingResponse.getMetadataMap().get(CommonConstants.Query.Response.MetadataKeys.RESPONSE_TYPE);
      if (responseType.equals(CommonConstants.Query.Response.ResponseType.DATA)) {
        // verify the returned data table metadata is empty.
        Map<String, String> metadata = dataTable.getMetadata();
        assertEquals(metadata.size(), 0);
        assertNotNull(dataTable.getDataSchema());
        numTotalDocs += dataTable.getNumberOfRows();
      } else {
        assertEquals(responseType, CommonConstants.Query.Response.ResponseType.METADATA);
        assertFalse(streamingResponses.hasNext());
        assertEquals(numTotalDocs, nonStreamResultDataTable.getNumberOfRows());
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
        // verify the returned data table metadata is empty.
        Map<String, String> metadata = dataTable.getMetadata();
        assertEquals(metadata.size(), 0);
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
}
