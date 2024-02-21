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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.pinot.broker.broker.helix.BaseBrokerStarter;
import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
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
import org.apache.pinot.spi.utils.CommonConstants.Minion;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
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
  protected static final int DEFAULT_BROKER_PORT = 18099;
  protected static final Random RANDOM = new Random(System.currentTimeMillis());

  protected List<BaseBrokerStarter> _brokerStarters;
  protected List<BaseServerStarter> _serverStarters;
  protected List<Integer> _brokerPorts;
  protected BaseMinionStarter _minionStarter;

  private String _brokerBaseApiUrl;

  private boolean _useMultiStageQueryEngine = false;

  private String _baseInstanceDataDir =
      System.getProperty("java.io.tmpdir") + File.separator + System.currentTimeMillis();

  private int _serverGrpcPort;
  private int _serverAdminApiPort;
  private int _serverNettyPort;

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

  protected PinotConfiguration getDefaultBrokerConfiguration() {
    return new PinotConfiguration();
  }

  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    // Do nothing, to be overridden by tests if they need something specific
  }

  protected PinotConfiguration getBrokerConf(int brokerId) {
    PinotConfiguration brokerConf = getDefaultBrokerConfiguration();
    brokerConf.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    brokerConf.setProperty(Helix.CONFIG_OF_ZOOKEEPR_SERVER, getZkUrl());
    brokerConf.setProperty(Broker.CONFIG_OF_BROKER_TIMEOUT_MS, 60 * 1000L);
    brokerConf.setProperty(Helix.KEY_OF_BROKER_QUERY_PORT,
        NetUtils.findOpenPort(DEFAULT_BROKER_PORT + brokerId + RandomUtils.nextInt(10000)));
    brokerConf.setProperty(Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, 0);
    overrideBrokerConf(brokerConf);
    return brokerConf;
  }

  protected void startBroker()
      throws Exception {
    startBrokers(1);
  }

  protected void startBrokers(int numBrokers)
      throws Exception {
    _brokerStarters = new ArrayList<>(numBrokers);
    _brokerPorts = new ArrayList<>();
    for (int i = 0; i < numBrokers; i++) {
      BaseBrokerStarter brokerStarter = startOneBroker(i);
      _brokerStarters.add(brokerStarter);
      _brokerPorts.add(brokerStarter.getPort());
    }
    _brokerBaseApiUrl = "http://localhost:" + _brokerPorts.get(0);
  }

  protected BaseBrokerStarter startOneBroker(int brokerId)
      throws Exception {
    HelixBrokerStarter brokerStarter = new HelixBrokerStarter();
    brokerStarter.init(getBrokerConf(brokerId));
    brokerStarter.start();
    return brokerStarter;
  }

  protected void startBrokerHttps()
      throws Exception {
    _brokerStarters = new ArrayList<>();
    _brokerPorts = new ArrayList<>();

    PinotConfiguration brokerConf = getDefaultBrokerConfiguration();
    brokerConf.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    brokerConf.setProperty(Helix.CONFIG_OF_ZOOKEEPR_SERVER, getZkUrl());
    brokerConf.setProperty(Broker.CONFIG_OF_BROKER_TIMEOUT_MS, 60 * 1000L);
    brokerConf.setProperty(Broker.CONFIG_OF_BROKER_HOSTNAME, LOCAL_HOST);
    brokerConf.setProperty(Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, 0);
    overrideBrokerConf(brokerConf);

    HelixBrokerStarter brokerStarter = new HelixBrokerStarter();
    brokerStarter.init(brokerConf);
    brokerStarter.start();
    _brokerStarters.add(brokerStarter);
    _brokerPorts.add(brokerStarter.getPort());
    if (brokerConf.containsKey("pinot.broker.client.access.protocols.internal.port")) {
      _brokerPorts.add(Integer.parseInt(brokerConf.getProperty("pinot.broker.client.access.protocols.internal.port")));
    }
    if (brokerConf.containsKey("pinot.broker.client.access.protocols.external.port")) {
      _brokerPorts.add(Integer.parseInt(brokerConf.getProperty("pinot.broker.client.access.protocols.external.port")));
    }
    // TLS port is the last one in the list
    _brokerBaseApiUrl = "https://localhost:" + _brokerPorts.get(_brokerPorts.size() - 1);
  }

  protected int getRandomBrokerPort() {
    return _brokerPorts.get(RANDOM.nextInt(_brokerPorts.size()));
  }

  protected PinotConfiguration getDefaultServerConfiguration() {
    PinotConfiguration serverConf = new PinotConfiguration();
    serverConf.setProperty(Helix.KEY_OF_SERVER_NETTY_HOST, LOCAL_HOST);
    serverConf.setProperty(Server.CONFIG_OF_SEGMENT_FORMAT_VERSION, "v3");
    serverConf.setProperty(Server.CONFIG_OF_SHUTDOWN_ENABLE_QUERY_CHECK, false);
    return serverConf;
  }

  protected void overrideServerConf(PinotConfiguration serverConf) {
    // Do nothing, to be overridden by tests if they need something specific
  }

  protected PinotConfiguration getServerConf(int serverId) {
    PinotConfiguration serverConf = getDefaultServerConfiguration();
    serverConf.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    serverConf.setProperty(Helix.CONFIG_OF_ZOOKEEPR_SERVER, getZkUrl());
    serverConf.setProperty(Server.CONFIG_OF_INSTANCE_DATA_DIR,
        _baseInstanceDataDir + File.separator + "PinotServer" + File.separator + "dataDir-" + serverId);
    serverConf.setProperty(Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR,
        _baseInstanceDataDir + File.separator + "PinotServer" + File.separator + "segmentTar-" + serverId);

    _serverAdminApiPort = NetUtils.findOpenPort(Server.DEFAULT_ADMIN_API_PORT + new Random().nextInt(10000) + serverId);
    serverConf.setProperty(Server.CONFIG_OF_ADMIN_API_PORT, _serverAdminApiPort);
    _serverNettyPort = NetUtils.findOpenPort(Helix.DEFAULT_SERVER_NETTY_PORT + new Random().nextInt(10000) + serverId);
    serverConf.setProperty(Helix.KEY_OF_SERVER_NETTY_PORT, _serverNettyPort);
    _serverGrpcPort =
        NetUtils.findOpenPort(Server.DEFAULT_GRPC_PORT + new Random().nextInt(10000) + serverId);
    serverConf.setProperty(Server.CONFIG_OF_GRPC_PORT, _serverGrpcPort);

    // Thread time measurement is disabled by default, enable it in integration tests.
    // TODO: this can be removed when we eventually enable thread time measurement by default.
    serverConf.setProperty(Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    overrideServerConf(serverConf);
    return serverConf;
  }

  protected void startServer()
      throws Exception {
    startServers(1);
  }

  protected void startServers(int numServers)
      throws Exception {
    FileUtils.deleteQuietly(new File(_baseInstanceDataDir + File.separator + "PinotServer"));
    _serverStarters = new ArrayList<>(numServers);
    for (int i = 0; i < numServers; i++) {
      _serverStarters.add(startOneServer(i));
    }
  }

  protected BaseServerStarter startOneServer(int serverId)
      throws Exception {
    HelixServerStarter serverStarter = new HelixServerStarter();
    serverStarter.init(getServerConf(serverId));
    serverStarter.start();
    return serverStarter;
  }

  protected BaseServerStarter startOneServer(PinotConfiguration serverConfig)
      throws Exception {
    HelixServerStarter serverStarter = new HelixServerStarter();
    serverStarter.init(serverConfig);
    serverStarter.start();
    return serverStarter;
  }

  protected void startServerHttps()
      throws Exception {
    FileUtils.deleteQuietly(new File(_baseInstanceDataDir + File.separator + "PinotServer"));
    _serverStarters = new ArrayList<>();

    PinotConfiguration serverConf = getDefaultServerConfiguration();
    serverConf.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    serverConf.setProperty(Helix.CONFIG_OF_ZOOKEEPR_SERVER, getZkUrl());
    overrideServerConf(serverConf);

    HelixServerStarter serverStarter = new HelixServerStarter();
    serverStarter.init(serverConf);
    serverStarter.start();
    _serverStarters.add(serverStarter);
  }

  protected PinotConfiguration getDefaultMinionConfiguration() {
    return new PinotConfiguration();
  }

  // NOTE: We don't allow multiple Minion instances in the same JVM because Minion uses singleton class MinionContext
  //       to manage the instance level configs
  protected void startMinion()
      throws Exception {
    FileUtils.deleteQuietly(new File(_baseInstanceDataDir + File.separator + "PinotMinion"));
    PinotConfiguration minionConf = getDefaultMinionConfiguration();
    minionConf.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    minionConf.setProperty(Helix.CONFIG_OF_ZOOKEEPR_SERVER, getZkUrl());
    minionConf.setProperty(CommonConstants.Helix.KEY_OF_MINION_PORT,
        NetUtils.findOpenPort(CommonConstants.Minion.DEFAULT_HELIX_PORT));
    _minionStarter = new MinionStarter();
    _minionStarter.init(minionConf);
    _minionStarter.start();
  }

  protected void stopBroker() {
    assertNotNull(_brokerStarters, "Brokers are not started");
    for (BaseBrokerStarter brokerStarter : _brokerStarters) {
      brokerStarter.stop();
    }
    _brokerStarters = null;
    _brokerPorts = null;
    _brokerBaseApiUrl = null;
  }

  protected void stopServer() {
    assertNotNull(_serverStarters, "Servers are not started");
    for (BaseServerStarter serverStarter : _serverStarters) {
      serverStarter.stop();
    }
    FileUtils.deleteQuietly(new File(_baseInstanceDataDir + File.separator + "PinotServer"));
    _serverStarters = null;
    _serverGrpcPort = 0;
    _serverAdminApiPort = 0;
    _serverNettyPort = 0;
  }

  protected void stopMinion() {
    assertNotNull(_minionStarter, "Minion is not started");
    _minionStarter.stop();
    FileUtils.deleteQuietly(new File(Minion.DEFAULT_INSTANCE_BASE_DIR));
    _minionStarter = null;
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
    uploadSegments(tableName, tableType, Collections.singletonList(tarDir));
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
    List<Header> headers = ImmutableList.of(new BasicHeader(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI,
        "file://" + segmentTarFile.getParentFile().getAbsolutePath() + "/" + URLEncoder.encode(segmentTarFile.getName(),
            StandardCharsets.UTF_8.toString())), new BasicHeader(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE,
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
  protected JsonNode postQuery(String query)
      throws Exception {
    return postQuery(query, getBrokerQueryApiUrl(getBrokerBaseApiUrl(), useMultiStageQueryEngine()), null,
        getExtraQueryProperties());
  }

  protected Map<String, String> getExtraQueryProperties() {
    return Collections.emptyMap();
  }

  /**
   * Queries the broker's sql query endpoint (/query or /query/sql)
   */
  public static JsonNode postQuery(String query, String brokerQueryApiUrl, Map<String, String> headers,
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
  protected JsonNode postQueryWithOptions(String query, String queryOptions)
      throws Exception {
    return postQuery(query, getBrokerQueryApiUrl(getBrokerBaseApiUrl(), useMultiStageQueryEngine()), null,
        ImmutableMap.of("queryOptions", queryOptions));
  }

  /**
   * Queries the controller's sql query endpoint (/sql)
   */
  public JsonNode postQueryToController(String query)
      throws Exception {
    return postQueryToController(query, getControllerBaseApiUrl(), null, getExtraQueryPropertiesForController());
  }

  private Map<String, String> getExtraQueryPropertiesForController() {
    if (!useMultiStageQueryEngine()) {
      return Collections.emptyMap();
    }
    return ImmutableMap.of("queryOptions", "useMultistageEngine=true");
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
        {"$docId"},
        {"$hostName"},
        {"$segmentName"}
    };
  }

  @DataProvider(name = "useBothQueryEngines")
  public Object[][] useBothQueryEngines() {
    return new Object[][]{
        {false},
        {true}
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
