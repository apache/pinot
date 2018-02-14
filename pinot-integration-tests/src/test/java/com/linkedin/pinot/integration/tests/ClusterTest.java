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
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.broker.broker.BrokerTestUtils;
import com.linkedin.pinot.broker.broker.helix.HelixBrokerStarter;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.TableTaskConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource.Realtime.Kafka;
import com.linkedin.pinot.common.utils.CommonConstants.Minion;
import com.linkedin.pinot.common.utils.CommonConstants.Server;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerTest;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.realtime.impl.kafka.AvroRecordToPinotRowGenerator;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaMessageDecoder;
import com.linkedin.pinot.core.util.AvroUtils;
import com.linkedin.pinot.minion.MinionStarter;
import com.linkedin.pinot.minion.executor.PinotTaskExecutor;
import com.linkedin.pinot.server.starter.helix.DefaultHelixStarterServerConfig;
import com.linkedin.pinot.server.starter.helix.HelixServerStarter;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;


/**
 * Base class for integration tests that involve a complete Pinot cluster.
 */
public abstract class ClusterTest extends ControllerTest {
  private static final int DEFAULT_BROKER_PORT = 18099;

  protected final String _clusterName = getHelixClusterName();
  protected String _brokerBaseApiUrl;

  private List<HelixBrokerStarter> _brokerStarters = new ArrayList<>();
  private List<HelixServerStarter> _serverStarters = new ArrayList<>();
  private List<MinionStarter> _minionStarters = new ArrayList<>();

  protected void startBroker() {
    startBrokers(1);
  }

  protected void startBroker(int basePort, String zkStr) {
    startBrokers(1, basePort, zkStr);
  }

  protected void startBrokers(int numBrokers) {
    startBrokers(numBrokers, DEFAULT_BROKER_PORT, ZkStarter.DEFAULT_ZK_STR);
  }

  protected void startBrokers(int numBrokers, int basePort, String zkStr) {
    _brokerBaseApiUrl = "http://localhost:" + basePort;
    for (int i = 0; i < numBrokers; i++) {
      Configuration configuration = BrokerTestUtils.getDefaultBrokerConfiguration();
      configuration.setProperty("pinot.broker.timeoutMs", 100 * 1000L);
      configuration.setProperty("pinot.broker.client.queryPort", Integer.toString(basePort + i));
      configuration.setProperty("pinot.broker.routing.table.builder.class", "random");
      configuration.setProperty("pinot.broker.delayShutdownTimeMs", 0);
      overrideBrokerConf(configuration);
      _brokerStarters.add(BrokerTestUtils.startBroker(_clusterName, zkStr, configuration));
    }
  }

  public static Configuration getDefaultServerConfiguration() {
    Configuration configuration = DefaultHelixStarterServerConfig.loadDefaultServerConf();
    configuration.setProperty(Helix.KEY_OF_SERVER_NETTY_HOST, LOCAL_HOST);
    configuration.setProperty(Server.CONFIG_OF_SEGMENT_FORMAT_VERSION, "v3");
    configuration.addProperty(Server.CONFIG_OF_ENABLE_DEFAULT_COLUMNS, true);
    configuration.setProperty(Server.CONFIG_OF_ENABLE_SHUTDOWN_DELAY, false);
    return configuration;
  }

  protected void startServer() {
    startServers(1);
  }

  protected void startServer(Configuration configuration) {
    startServers(1, configuration, Server.DEFAULT_ADMIN_API_PORT, Helix.DEFAULT_SERVER_NETTY_PORT,
        ZkStarter.DEFAULT_ZK_STR);
  }

  protected void startServers(int numServers) {
    startServers(numServers, getDefaultServerConfiguration(), Server.DEFAULT_ADMIN_API_PORT,
        Helix.DEFAULT_SERVER_NETTY_PORT, ZkStarter.DEFAULT_ZK_STR);
  }

  protected void startServers(int numServers, int baseAdminApiPort, int baseNettyPort, String zkStr) {
    startServers(numServers, getDefaultServerConfiguration(), baseAdminApiPort, baseNettyPort, zkStr);
  }

  protected void startServers(int numServers, Configuration configuration, int baseAdminApiPort, int baseNettyPort,
      String zkStr) {
    try {
      for (int i = 0; i < numServers; i++) {
        configuration.setProperty(Server.CONFIG_OF_INSTANCE_DATA_DIR, Server.DEFAULT_INSTANCE_DATA_DIR + "-" + i);
        configuration.setProperty(Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR,
            Server.DEFAULT_INSTANCE_SEGMENT_TAR_DIR + "-" + i);
        configuration.setProperty(Server.CONFIG_OF_ADMIN_API_PORT, baseAdminApiPort - i);
        configuration.setProperty(Server.CONFIG_OF_NETTY_PORT, baseNettyPort + i);
        overrideServerConf(configuration);
        _serverStarters.add(new HelixServerStarter(_clusterName, zkStr, configuration));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void startMinion() {
    startMinions(1, null);
  }

  protected void startMinions(int minionCount,
      @Nullable Map<String, Class<? extends PinotTaskExecutor>> taskExecutorsToRegister) {
    try {
      for (int i = 0; i < minionCount; i++) {
        Configuration config = new PropertiesConfiguration();
        config.setProperty(Helix.Instance.INSTANCE_ID_KEY,
            Minion.INSTANCE_PREFIX + "minion" + i + "_" + (Minion.DEFAULT_HELIX_PORT + i));
        config.setProperty(Helix.Instance.DATA_DIR_KEY, Minion.DEFAULT_INSTANCE_DATA_DIR + "-" + i);
        MinionStarter minionStarter = new MinionStarter(ZkStarter.DEFAULT_ZK_STR, _clusterName, config);

        // Register plug-in task executors
        if (taskExecutorsToRegister != null) {
          for (Map.Entry<String, Class<? extends PinotTaskExecutor>> entry : taskExecutorsToRegister.entrySet()) {
            minionStarter.registerTaskExecutorClass(entry.getKey(), entry.getValue());
          }
        }

        minionStarter.start();
        _minionStarters.add(minionStarter);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void overrideServerConf(Configuration configuration) {
    // Do nothing, to be overridden by tests if they need something specific
  }

  protected void overrideBrokerConf(Configuration configuration) {
    // Do nothing, to be overridden by tests if they need something specific
  }

  protected void stopBroker() {
    for (HelixBrokerStarter brokerStarter : _brokerStarters) {
      BrokerTestUtils.stopBroker(brokerStarter);
    }
  }

  protected void stopServer() {
    for (HelixServerStarter helixServerStarter : _serverStarters) {
      helixServerStarter.stop();
    }
    FileUtils.deleteQuietly(new File(Server.DEFAULT_INSTANCE_BASE_DIR));
  }

  protected void stopMinion() {
    for (MinionStarter minionStarter : _minionStarters) {
      minionStarter.stop();
    }
    FileUtils.deleteQuietly(new File(Minion.DEFAULT_INSTANCE_BASE_DIR));
  }

  protected void addSchema(File schemaFile, String schemaName) throws Exception {
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      fileUploadDownloadClient.addSchema(FileUploadDownloadClient.getUploadSchemaHttpURI(LOCAL_HOST, _controllerPort),
          schemaName, schemaFile);
    }
  }

  /**
   * Upload all segments inside the given directory to the cluster.
   *
   * @param segmentDir Segment directory
   */
  protected void uploadSegments(@Nonnull File segmentDir) throws Exception {
    String[] segmentNames = segmentDir.list();
    Assert.assertNotNull(segmentNames);
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      final URI uploadSegmentHttpURI = FileUploadDownloadClient.getUploadSegmentHttpURI(LOCAL_HOST, _controllerPort);

      // Upload all segments in parallel
      int numSegments = segmentNames.length;
      ExecutorService executor = Executors.newFixedThreadPool(numSegments);
      List<Future<Integer>> tasks = new ArrayList<>(numSegments);
      for (final String segmentName : segmentNames) {
        final File segmentFile = new File(segmentDir, segmentName);
        tasks.add(executor.submit(new Callable<Integer>() {
          @Override
          public Integer call() throws Exception {
            return fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, segmentName, segmentFile);
          }
        }));
      }
      for (Future<Integer> task : tasks) {
        Assert.assertEquals((int) task.get(), HttpStatus.SC_OK);
      }
      executor.shutdown();
    }
  }

  protected void addOfflineTable(String tableName) throws Exception {
    addOfflineTable(tableName, SegmentVersion.v1);
  }

  protected void addOfflineTable(String tableName, SegmentVersion segmentVersion) throws Exception {
    addOfflineTable(tableName, null, null, null, null, null, segmentVersion, null, null);
  }

  protected void addOfflineTable(String tableName, String timeColumnName, String timeType, String brokerTenant,
      String serverTenant, String loadMode, SegmentVersion segmentVersion, List<String> invertedIndexColumns,
      TableTaskConfig taskConfig) throws Exception {
    TableConfig tableConfig =
        getOfflineTableConfig(tableName, timeColumnName, timeType, brokerTenant, serverTenant, loadMode, segmentVersion,
            invertedIndexColumns, taskConfig);
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJSONConfigString());
  }

  protected void updateOfflineTable(String tableName, String timeColumnName, String timeType, String brokerTenant,
      String serverTenant, String loadMode, SegmentVersion segmentVersion, List<String> invertedIndexColumns,
      TableTaskConfig taskConfig) throws Exception {
    TableConfig tableConfig =
        getOfflineTableConfig(tableName, timeColumnName, timeType, brokerTenant, serverTenant, loadMode, segmentVersion,
            invertedIndexColumns, taskConfig);
    sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig(tableName), tableConfig.toJSONConfigString());
  }

  private static TableConfig getOfflineTableConfig(String tableName, String timeColumnName, String timeType,
      String brokerTenant, String serverTenant, String loadMode, SegmentVersion segmentVersion,
      List<String> invertedIndexColumns, TableTaskConfig taskConfig) throws Exception {
    return new TableConfig.Builder(Helix.TableType.OFFLINE).setTableName(tableName)
        .setTimeColumnName(timeColumnName)
        .setTimeType(timeType)
        .setNumReplicas(3)
        .setBrokerTenant(brokerTenant)
        .setServerTenant(serverTenant)
        .setLoadMode(loadMode)
        .setSegmentVersion(segmentVersion.toString())
        .setInvertedIndexColumns(invertedIndexColumns)
        .setTaskConfig(taskConfig)
        .build();
  }

  protected void dropOfflineTable(String tableName) throws Exception {
    sendDeleteRequest(
        _controllerRequestURLBuilder.forTableDelete(TableNameBuilder.OFFLINE.tableNameWithType(tableName)));
  }

  public static class AvroFileSchemaKafkaAvroMessageDecoder implements KafkaMessageDecoder<byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroFileSchemaKafkaAvroMessageDecoder.class);
    public static File avroFile;
    private org.apache.avro.Schema _avroSchema;
    private AvroRecordToPinotRowGenerator _rowGenerator;
    private DecoderFactory _decoderFactory = new DecoderFactory();
    private DatumReader<GenericData.Record> _reader;

    @Override
    public void init(Map<String, String> props, Schema indexingSchema, String kafkaTopicName) throws Exception {
      // Load Avro schema
      DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile);
      _avroSchema = reader.getSchema();
      reader.close();
      _rowGenerator = new AvroRecordToPinotRowGenerator(indexingSchema);
      _reader = new GenericDatumReader<>(_avroSchema);
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
        return _rowGenerator.transform(avroRecord, destination);
      } catch (Exception e) {
        LOGGER.error("Caught exception", e);
        throw new RuntimeException(e);
      }
    }
  }

  protected void addRealtimeTable(String tableName, boolean useLlc, String kafkaBrokerList, String kafkaZkUrl,
      String kafkaTopic, int realtimeSegmentFlushSize, File avroFile, String timeColumnName, String timeType,
      String schemaName, String brokerTenant, String serverTenant, String loadMode, String sortedColumn,
      List<String> invertedIndexColumns, List<String> noDictionaryColumns, TableTaskConfig taskConfig,
      String kafkaConsumerFactoryName) throws Exception {
    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put("streamType", "kafka");
    if (useLlc) {
      // LLC
      streamConfigs.put(DataSource.STREAM_PREFIX + "." + Kafka.CONSUMER_TYPE, Kafka.ConsumerType.simple.toString());
      streamConfigs.put(DataSource.STREAM_PREFIX + "." + Kafka.KAFKA_BROKER_LIST, kafkaBrokerList);
      streamConfigs.put(DataSource.STREAM_PREFIX + "." + Kafka.CONSUMER_FACTORY, kafkaConsumerFactoryName);
    } else {
      // HLC
      streamConfigs.put(DataSource.STREAM_PREFIX + "." + Kafka.CONSUMER_TYPE, Kafka.ConsumerType.highLevel.toString());
      streamConfigs.put(DataSource.STREAM_PREFIX + "." + Kafka.ZK_BROKER_URL, kafkaZkUrl);
      streamConfigs.put(DataSource.STREAM_PREFIX + "." + Kafka.HighLevelConsumer.ZK_CONNECTION_STRING, kafkaZkUrl);
    }
    streamConfigs.put(DataSource.STREAM_PREFIX + "." + Kafka.TOPIC_NAME, kafkaTopic);
    AvroFileSchemaKafkaAvroMessageDecoder.avroFile = avroFile;
    streamConfigs.put(DataSource.STREAM_PREFIX + "." + Kafka.DECODER_CLASS,
        AvroFileSchemaKafkaAvroMessageDecoder.class.getName());
    streamConfigs.put(DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE, Integer.toString(realtimeSegmentFlushSize));
    streamConfigs.put(
        DataSource.STREAM_PREFIX + "." + Kafka.KAFKA_CONSUMER_PROPS_PREFIX + "." + Kafka.AUTO_OFFSET_RESET, "smallest");

    TableConfig tableConfig = new TableConfig.Builder(Helix.TableType.REALTIME).setTableName(tableName)
        .setLLC(useLlc)
        .setTimeColumnName(timeColumnName)
        .setTimeType(timeType)
        .setSchemaName(schemaName)
        .setBrokerTenant(brokerTenant)
        .setServerTenant(serverTenant)
        .setLoadMode(loadMode)
        .setSortedColumn(sortedColumn)
        .setInvertedIndexColumns(invertedIndexColumns)
        .setNoDictionaryColumns(noDictionaryColumns)
        .setStreamConfigs(streamConfigs)
        .setTaskConfig(taskConfig)
        .build();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJSONConfigString());
  }

  protected void dropRealtimeTable(String tableName) throws Exception {
    sendDeleteRequest(
        _controllerRequestURLBuilder.forTableDelete(TableNameBuilder.REALTIME.tableNameWithType(tableName)));
  }

  protected void addHybridTable(String tableName, boolean useLlc, String kafkaBrokerList, String kafkaZkUrl,
      String kafkaTopic, int realtimeSegmentFlushSize, File avroFile, String timeColumnName, String timeType,
      String schemaName, String brokerTenant, String serverTenant, String loadMode, String sortedColumn,
      List<String> invertedIndexColumns, List<String> noDictionaryColumns, TableTaskConfig taskConfig)
      throws Exception {
    addOfflineTable(tableName, timeColumnName, timeType, brokerTenant, serverTenant, loadMode, SegmentVersion.v1,
        invertedIndexColumns, taskConfig);
    addRealtimeTable(tableName, useLlc, kafkaBrokerList, kafkaZkUrl, kafkaTopic, realtimeSegmentFlushSize, avroFile,
        timeColumnName, timeType, schemaName, brokerTenant, serverTenant, loadMode, sortedColumn, invertedIndexColumns,
        noDictionaryColumns, taskConfig, null);
  }

  protected void createBrokerTenant(String tenantName, int brokerCount) throws Exception {
    JSONObject request = ControllerRequestBuilderUtil.buildBrokerTenantCreateRequestJSON(tenantName, brokerCount);
    sendPostRequest(_controllerRequestURLBuilder.forBrokerTenantCreate(), request.toString());
  }

  protected void createServerTenant(String tenantName, int offlineServerCount, int realtimeServerCount)
      throws Exception {
    JSONObject request = ControllerRequestBuilderUtil.buildServerTenantCreateRequestJSON(tenantName,
        offlineServerCount + realtimeServerCount, offlineServerCount, realtimeServerCount);
    sendPostRequest(_controllerRequestURLBuilder.forServerTenantCreate(), request.toString());
  }

  protected JSONObject getDebugInfo(final String uri) throws Exception {
    return new JSONObject(sendGetRequest(_brokerBaseApiUrl + "/" + uri));
  }

  protected JSONObject postQuery(String query) throws Exception {
    return postQuery(query, _brokerBaseApiUrl);
  }

  public static JSONObject postQuery(String query, String brokerBaseApiUrl) throws Exception {
    return postQuery(query, brokerBaseApiUrl, false);
  }

  public static JSONObject postQuery(String query, String brokerBaseApiUrl, boolean enableTrace) throws Exception {
    JSONObject payload = new JSONObject();
    payload.put("pql", query);
    payload.put("trace", enableTrace);

    return new JSONObject(sendPostRequest(brokerBaseApiUrl + "/query", payload.toString()));
  }
}
