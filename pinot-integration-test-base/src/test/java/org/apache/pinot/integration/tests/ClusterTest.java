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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.pinot.broker.broker.helix.BaseBrokerStarter;
import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.minion.BaseMinionStarter;
import org.apache.pinot.minion.MinionStarter;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractorConfig;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.server.starter.helix.HelixServerStarter;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.intellij.lang.annotations.Language;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;

import static org.apache.pinot.integration.tests.ClusterIntegrationTestUtils.getBrokerQueryApiUrl;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Base class for integration tests that involve a complete Pinot cluster.
 */
@Listeners(NettyTestNGListener.class)
public abstract class ClusterTest extends ControllerTest {
  protected static final String TEMP_DIR =
      FileUtils.getTempDirectoryPath() + File.separator + System.currentTimeMillis();
  protected static final String TEMP_SERVER_DIR = TEMP_DIR + File.separator + "PinotServer";
  protected static final String TEMP_MINION_DIR = TEMP_DIR + File.separator + "PinotMinion";
  protected static final Random RANDOM = new Random(System.currentTimeMillis());

  protected final List<BaseBrokerStarter> _brokerStarters = new ArrayList<>();
  protected final List<Integer> _brokerPorts = new ArrayList<>();
  protected String _brokerBaseApiUrl;

  protected final List<BaseServerStarter> _serverStarters = new ArrayList<>();
  protected int _serverGrpcPort;
  protected int _serverAdminApiPort;
  protected int _serverNettyPort;

  protected BaseMinionStarter _minionStarter;
  protected String _minionBaseApiUrl;

  protected boolean _useMultiStageQueryEngine = false;

  protected int getServerGrpcPort() {
    return _serverGrpcPort;
  }

  protected int getServerAdminApiPort() {
    return _serverAdminApiPort;
  }

  protected int getServerNettyPort() {
    return _serverNettyPort;
  }

  protected String getBrokerBaseApiUrl() {
    return _brokerBaseApiUrl;
  }

  public String getMinionBaseApiUrl() {
    return _minionBaseApiUrl;
  }

  protected boolean useMultiStageQueryEngine() {
    return _useMultiStageQueryEngine;
  }

  protected void setUseMultiStageQueryEngine(boolean useMultiStageQueryEngine) {
    _useMultiStageQueryEngine = useMultiStageQueryEngine;
  }

  protected void disableMultiStageQueryEngine() {
    setUseMultiStageQueryEngine(false);
  }

  protected void enableMultiStageQueryEngine() {
    setUseMultiStageQueryEngine(true);
  }

  /**
   * Can be overridden to add more properties.
   */
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
  }

  /**
   * Can be overridden to use a different implementation.
   */
  protected BaseBrokerStarter createBrokerStarter() {
    return new HelixBrokerStarter();
  }

  protected PinotConfiguration getBrokerConf(int brokerId) {
    PinotConfiguration brokerConf = new PinotConfiguration();
    brokerConf.setProperty(Helix.CONFIG_OF_ZOOKEEPR_SERVER, getZkUrl());
    brokerConf.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    brokerConf.setProperty(Broker.CONFIG_OF_BROKER_HOSTNAME, LOCAL_HOST);
    int brokerPort = NetUtils.findOpenPort(_nextBrokerPort);
    brokerConf.setProperty(Helix.KEY_OF_BROKER_QUERY_PORT, brokerPort);
    _brokerPorts.add(brokerPort);
    if (_brokerBaseApiUrl == null) {
      _brokerBaseApiUrl = "http://localhost:" + brokerPort;
    }
    _nextBrokerPort = brokerPort + 1;
    brokerConf.setProperty(Broker.CONFIG_OF_BROKER_TIMEOUT_MS, 60 * 1000L);
    brokerConf.setProperty(Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, 0);
    brokerConf.setProperty(CommonConstants.CONFIG_OF_TIMEZONE, "UTC");
    overrideBrokerConf(brokerConf);
    return brokerConf;
  }

  protected void startBroker()
      throws Exception {
    startBrokers(1);
  }

  protected void startBrokers(int numBrokers)
      throws Exception {
    runWithHelixMock(() -> {
      for (int i = 0; i < numBrokers; i++) {
        BaseBrokerStarter brokerStarter = startOneBroker(i);
        _brokerStarters.add(brokerStarter);
      }
      assertEquals(System.getProperty("user.timezone"), "UTC");
    });
  }

  protected BaseBrokerStarter startOneBroker(int brokerId)
      throws Exception {
    BaseBrokerStarter brokerStarter = createBrokerStarter();
    brokerStarter.init(getBrokerConf(brokerId));
    brokerStarter.start();
    return brokerStarter;
  }

  protected int getRandomBrokerPort() {
    return _brokerPorts.get(RANDOM.nextInt(_brokerPorts.size()));
  }

  /**
   * Can be overridden to add more properties.
   */
  protected void overrideServerConf(PinotConfiguration serverConf) {
  }

  /**
   * Can be overridden to use a different implementation.
   */
  protected BaseServerStarter createServerStarter() {
    return new HelixServerStarter();
  }

  protected PinotConfiguration getServerConf(int serverId) {
    PinotConfiguration serverConf = new PinotConfiguration();
    serverConf.setProperty(Helix.CONFIG_OF_ZOOKEEPR_SERVER, getZkUrl());
    serverConf.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    serverConf.setProperty(Helix.KEY_OF_SERVER_NETTY_HOST, LOCAL_HOST);
    serverConf.setProperty(Server.CONFIG_OF_INSTANCE_DATA_DIR,
        TEMP_SERVER_DIR + File.separator + "dataDir-" + serverId);
    serverConf.setProperty(Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR,
        TEMP_SERVER_DIR + File.separator + "segmentTar-" + serverId);
    serverConf.setProperty(Server.CONFIG_OF_SEGMENT_FORMAT_VERSION, "v3");
    serverConf.setProperty(Server.CONFIG_OF_SHUTDOWN_ENABLE_QUERY_CHECK, false);

    int serverAdminApiPort = NetUtils.findOpenPort(_nextServerPort);
    serverConf.setProperty(Server.CONFIG_OF_ADMIN_API_PORT, serverAdminApiPort);
    int serverNettyPort = NetUtils.findOpenPort(serverAdminApiPort + 1);
    serverConf.setProperty(Helix.KEY_OF_SERVER_NETTY_PORT, serverNettyPort);
    int serverGrpcPort = NetUtils.findOpenPort(serverNettyPort + 1);
    serverConf.setProperty(Server.CONFIG_OF_GRPC_PORT, serverGrpcPort);
    if (_serverAdminApiPort == 0) {
      _serverAdminApiPort = serverAdminApiPort;
      _serverNettyPort = serverNettyPort;
      _serverGrpcPort = serverGrpcPort;
    }
    _nextServerPort = serverGrpcPort + 1;

    // Thread time measurement is disabled by default, enable it in integration tests.
    // TODO: this can be removed when we eventually enable thread time measurement by default.
    serverConf.setProperty(Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    serverConf.setProperty(CommonConstants.CONFIG_OF_TIMEZONE, "UTC");
    overrideServerConf(serverConf);
    return serverConf;
  }

  protected void startServer()
      throws Exception {
    startServers(1);
  }

  protected void startServers(int numServers)
      throws Exception {
    runWithHelixMock(() -> {
      FileUtils.deleteQuietly(new File(TEMP_SERVER_DIR));
      for (int i = 0; i < numServers; i++) {
        _serverStarters.add(startOneServer(i));
      }
      assertEquals(System.getProperty("user.timezone"), "UTC");
    });
  }

  protected BaseServerStarter startOneServer(int serverId)
      throws Exception {
    return startOneServer(getServerConf(serverId));
  }

  protected BaseServerStarter startOneServer(PinotConfiguration serverConfig)
      throws Exception {
    BaseServerStarter serverStarter = createServerStarter();
    serverStarter.init(serverConfig);
    serverStarter.start();
    return serverStarter;
  }

  /**
   * Can be overridden to add more properties.
   */
  protected void overrideMinionConf(PinotConfiguration minionConf) {
  }

  /**
   * Can be overridden to use a different implementation.
   */
  protected BaseMinionStarter createMinionStarter() {
    return new MinionStarter();
  }

  protected PinotConfiguration getMinionConf() {
    PinotConfiguration minionConf = new PinotConfiguration();
    minionConf.setProperty(Helix.CONFIG_OF_ZOOKEEPR_SERVER, getZkUrl());
    minionConf.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    minionConf.setProperty(Helix.KEY_OF_MINION_HOST, LOCAL_HOST);
    int minionPort = NetUtils.findOpenPort(_nextMinionPort);
    minionConf.setProperty(Helix.KEY_OF_MINION_PORT, minionPort);
    if (_minionBaseApiUrl == null) {
      _minionBaseApiUrl = "http://localhost:" + minionPort;
    }
    _nextMinionPort = minionPort + 1;
    minionConf.setProperty(Helix.Instance.DATA_DIR_KEY, TEMP_MINION_DIR + File.separator + "dataDir");
    minionConf.setProperty(CommonConstants.CONFIG_OF_TIMEZONE, "UTC");
    overrideMinionConf(minionConf);
    return minionConf;
  }

  // NOTE: We don't allow multiple Minion instances in the same JVM because Minion uses singleton class MinionContext
  //       to manage the instance level configs
  protected void startMinion()
      throws Exception {
    FileUtils.deleteQuietly(new File(TEMP_MINION_DIR));
    _minionStarter = createMinionStarter();
    _minionStarter.init(getMinionConf());
    _minionStarter.start();
    assertEquals(System.getProperty("user.timezone"), "UTC");
  }

  protected void stopBroker() {
    assertNotNull(_brokerStarters, "Brokers are not started");
    for (BaseBrokerStarter brokerStarter : _brokerStarters) {
      brokerStarter.stop();
    }
    _brokerStarters.clear();
    _brokerPorts.clear();
    _brokerBaseApiUrl = null;
  }

  protected void stopServer() {
    assertNotNull(_serverStarters, "Servers are not started");
    for (BaseServerStarter serverStarter : _serverStarters) {
      serverStarter.stop();
    }
    FileUtils.deleteQuietly(new File(TEMP_SERVER_DIR));
    _serverStarters.clear();
    _serverGrpcPort = 0;
    _serverAdminApiPort = 0;
    _serverNettyPort = 0;
  }

  protected void stopMinion() {
    assertNotNull(_minionStarter, "Minion is not started");
    _minionStarter.stop();
    FileUtils.deleteQuietly(new File(TEMP_MINION_DIR));
    _minionStarter = null;
    _minionBaseApiUrl = null;
  }

  protected void restartServers()
      throws Exception {
    assertNotNull(_serverStarters, "Servers are not started");
    List<BaseServerStarter> oldServers = new ArrayList<>(_serverStarters);
    int numServers = _serverStarters.size();
    _serverStarters.clear();
    for (int i = 0; i < numServers; i++) {
      _serverStarters.add(restartServer(oldServers.get(i)));
    }
  }

  protected BaseServerStarter restartServer(BaseServerStarter serverStarter)
      throws Exception {
    PinotConfiguration serverConfig = serverStarter.getConfig();
    serverStarter.stop();
    return startOneServer(serverConfig);
  }

  /**
   * Upload all segments inside the given directory to the cluster.
   */
  protected void uploadSegments(String tableName, File tarDir)
      throws Exception {
    uploadSegments(tableName, TableType.OFFLINE, tarDir);
  }

  /**
   * Upload all segments inside the given directory to the cluster.
   */
  protected void uploadSegments(String tableName, List<File> tarDirs)
      throws Exception {
    uploadSegments(tableName, TableType.OFFLINE, tarDirs);
  }

  /**
   * Upload all segments inside the given directory to the cluster.
   */
  protected void uploadSegments(String tableName, TableType tableType, File tarDir)
      throws Exception {
    uploadSegments(tableName, tableType, List.of(tarDir));
  }

  /**
   * Upload all segments inside the given directories to the cluster.
   */
  protected void uploadSegments(String tableName, TableType tableType, List<File> tarDirs)
      throws Exception {
    List<File> segmentTarFiles = new ArrayList<>();
    for (File tarDir : tarDirs) {
      File[] tarFiles = tarDir.listFiles();
      assertNotNull(tarFiles);
      Collections.addAll(segmentTarFiles, tarFiles);
    }
    int numSegments = segmentTarFiles.size();
    assertTrue(numSegments > 0);

    URI uploadSegmentHttpURI = URI.create(getControllerRequestURLBuilder().forSegmentUpload());
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      if (numSegments == 1) {
        File segmentTarFile = segmentTarFiles.get(0);
        if (System.currentTimeMillis() % 2 == 0) {
          assertEquals(
              fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(), segmentTarFile,
                  tableName, tableType).getStatusCode(), HttpStatus.SC_OK);
        } else {
          assertEquals(
              uploadSegmentWithOnlyMetadata(tableName, tableType, uploadSegmentHttpURI, fileUploadDownloadClient,
                  segmentTarFile), HttpStatus.SC_OK);
        }
      } else {
        // Upload all segments in parallel
        ExecutorService executorService = Executors.newFixedThreadPool(numSegments);
        List<Future<Integer>> futures = new ArrayList<>(numSegments);
        for (File segmentTarFile : segmentTarFiles) {
          futures.add(executorService.submit(() -> {
            if (System.currentTimeMillis() % 2 == 0) {
              return fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(),
                  segmentTarFile, tableName, tableType).getStatusCode();
            } else {
              return uploadSegmentWithOnlyMetadata(tableName, tableType, uploadSegmentHttpURI, fileUploadDownloadClient,
                  segmentTarFile);
            }
          }));
        }
        executorService.shutdown();
        for (Future<Integer> future : futures) {
          assertEquals((int) future.get(), HttpStatus.SC_OK);
        }
      }
    }
  }

  private int uploadSegmentWithOnlyMetadata(String tableName, TableType tableType, URI uploadSegmentHttpURI,
      FileUploadDownloadClient fileUploadDownloadClient, File segmentTarFile)
      throws IOException, HttpErrorStatusException {
    List<Header> headers = List.of(new BasicHeader(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI,
            "file://" + segmentTarFile.getParentFile().getAbsolutePath() + "/"
                + URIUtils.encode(segmentTarFile.getName())),
        new BasicHeader(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE,
            FileUploadDownloadClient.FileUploadType.METADATA.toString()));
    // Add table name and table type as request parameters
    NameValuePair tableNameValuePair =
        new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME, tableName);
    NameValuePair tableTypeValuePair =
        new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_TYPE, tableType.name());
    List<NameValuePair> parameters = Arrays.asList(tableNameValuePair, tableTypeValuePair);
    return fileUploadDownloadClient.uploadSegmentMetadata(uploadSegmentHttpURI, segmentTarFile.getName(),
        segmentTarFile, headers, parameters, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS).getStatusCode();
  }

  public static class AvroFileSchemaKafkaAvroMessageDecoder implements StreamMessageDecoder<byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroFileSchemaKafkaAvroMessageDecoder.class);
    public static File _avroFile;
    private RecordExtractor<GenericRecord> _recordExtractor;
    private final DecoderFactory _decoderFactory = new DecoderFactory();
    private DatumReader<GenericData.Record> _reader;

    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
        throws Exception {
      // Load Avro schema
      org.apache.avro.Schema avroSchema;
      try (DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(_avroFile)) {
        avroSchema = reader.getSchema();
      } catch (Exception ex) {
        LOGGER.error("Caught exception", ex);
        throw new RuntimeException(ex);
      }
      AvroRecordExtractorConfig config = new AvroRecordExtractorConfig();
      config.init(props);
      _recordExtractor = new AvroRecordExtractor();
      _recordExtractor.init(fieldsToRead, config);
      _reader = new GenericDatumReader<>(avroSchema);
    }

    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      return decode(payload, 0, payload.length, destination);
    }

    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      try {
        GenericData.Record avroRecord =
            _reader.read(null, _decoderFactory.binaryDecoder(payload, offset, length, null));
        return _recordExtractor.extract(avroRecord, destination);
      } catch (Exception e) {
        LOGGER.error("Caught exception", e);
        throw new RuntimeException(e);
      }
    }
  }

  protected JsonNode getDebugInfo(final String uri)
      throws Exception {
    return JsonUtils.stringToJsonNode(sendGetRequest(getBrokerBaseApiUrl() + "/" + uri));
  }

  /**
   * Queries the broker's sql query endpoint (/query/sql)
   */
  public JsonNode postQuery(@Language("sql") String query)
      throws Exception {
    return postQuery(query, getBrokerQueryApiUrl(getBrokerBaseApiUrl(), useMultiStageQueryEngine()), null,
        getExtraQueryProperties());
  }

  /**
   * Queries the broker's sql query endpoint (/query/sql)
   */
  protected JsonNode postQuery(@Language("sql") String query, Map<String, String> headers)
      throws Exception {
    return postQuery(query, getBrokerQueryApiUrl(getBrokerBaseApiUrl(), useMultiStageQueryEngine()), headers,
        getExtraQueryProperties());
  }

  protected Map<String, String> getExtraQueryProperties() {
    return Map.of();
  }

  /**
   * Queries the broker's sql query endpoint (/query or /query/sql)
   */
  public static JsonNode postQuery(@Language("sql") String query, String brokerQueryApiUrl, Map<String, String> headers,
      Map<String, String> extraJsonProperties)
      throws Exception {
    ObjectNode payload = JsonUtils.newObjectNode();
    payload.put("sql", query);
    if (MapUtils.isNotEmpty(extraJsonProperties)) {
      for (Map.Entry<String, String> extraProperty : extraJsonProperties.entrySet()) {
        payload.put(extraProperty.getKey(), extraProperty.getValue());
      }
    }
    return JsonUtils.stringToJsonNode(sendPostRequest(brokerQueryApiUrl, payload.toString(), headers));
  }

  /**
   * Queries the broker's sql query endpoint (/query/sql) using query and queryOptions strings
   */
  protected JsonNode postQueryWithOptions(@Language("sql") String query, String queryOptions)
      throws Exception {
    return postQuery(query, getBrokerQueryApiUrl(getBrokerBaseApiUrl(), useMultiStageQueryEngine()), null,
        Map.of("queryOptions", queryOptions));
  }

  /**
   * Queries the controller's sql query endpoint (/sql)
   */
  public JsonNode postQueryToController(@Language("sql") String query)
      throws Exception {
    return postQueryToController(query, getControllerBaseApiUrl(), null, getExtraQueryPropertiesForController());
  }

  public JsonNode cancelQuery(String clientQueryId)
    throws Exception {
    URI cancelURI = URI.create(getControllerRequestURLBuilder().forCancelQueryByClientId(clientQueryId));
    Object o = _httpClient.sendDeleteRequest(cancelURI);
    return null; // TODO
  }

  private Map<String, String> getExtraQueryPropertiesForController() {
    if (!useMultiStageQueryEngine()) {
      return Map.of();
    }
    return Map.of("queryOptions", "useMultistageEngine=true");
  }

  /**
   * Queries the controller's sql query endpoint (/sql)
   */
  public static JsonNode postQueryToController(String query, String controllerBaseApiUrl, Map<String, String> headers,
      Map<String, String> extraJsonProperties)
      throws Exception {
    ObjectNode payload = JsonUtils.newObjectNode();
    payload.put("sql", query);
    if (MapUtils.isNotEmpty(extraJsonProperties)) {
      for (Map.Entry<String, String> extraProperty : extraJsonProperties.entrySet()) {
        payload.put(extraProperty.getKey(), extraProperty.getValue());
      }
    }
    return JsonUtils.stringToJsonNode(
        sendPostRequest(controllerBaseApiUrl + "/sql", JsonUtils.objectToString(payload), headers));
  }

  public List<String> getColumns(JsonNode response) {
    JsonNode resultTableJson = response.get("resultTable");
    Assert.assertNotNull(resultTableJson, "'resultTable' is null");
    JsonNode dataSchemaJson = resultTableJson.get("dataSchema");
    Assert.assertNotNull(resultTableJson, "'resultTable.dataSchema' is null");
    JsonNode colNamesJson = dataSchemaJson.get("columnNames");
    Assert.assertNotNull(resultTableJson, "'resultTable.dataSchema.columnNames' is null");

    List<String> cols = new ArrayList<>();
    int i = 0;
    for (JsonNode jsonNode : colNamesJson) {
      String colName = jsonNode.textValue();
      Assert.assertNotNull(colName, "Column at index " + i + " is not a string");
      cols.add(colName);
      i++;
    }
    return cols;
  }

  public void assertNoError(JsonNode response) {
    JsonNode exceptionsJson = response.get("exceptions");
    Iterator<JsonNode> exIterator = exceptionsJson.iterator();
    if (exIterator.hasNext()) {
      Assert.fail("There is at least one exception: " + exIterator.next());
    }
  }

  @DataProvider(name = "systemColumns")
  public Object[][] systemColumns() {
    return new Object[][]{
        {"$docId"}, {"$hostName"}, {"$segmentName"}
    };
  }

  @DataProvider(name = "useBothQueryEngines")
  public Object[][] useBothQueryEngines() {
    return new Object[][]{
        {false}, {true}
    };
  }

  @DataProvider(name = "useV1QueryEngine")
  public Object[][] useV1QueryEngine() {
    return new Object[][]{
        {false}
    };
  }

  @DataProvider(name = "useV2QueryEngine")
  public Object[][] useV2QueryEngine() {
    return new Object[][]{
        {true}
    };
  }

  protected void notSupportedInV2() {
    if (useMultiStageQueryEngine()) {
      throw new SkipException("Some queries fail when using multi-stage engine");
    }
  }
}
