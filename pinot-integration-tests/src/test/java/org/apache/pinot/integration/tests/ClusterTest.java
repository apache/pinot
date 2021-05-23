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
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.io.FileUtils;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.minion.MinionStarter;
import org.apache.pinot.minion.event.MinionEventObserverFactory;
import org.apache.pinot.minion.executor.PinotTaskExecutorFactory;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.server.starter.helix.DefaultHelixStarterServerConfig;
import org.apache.pinot.server.starter.helix.HelixServerStarter;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Minion;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Base class for integration tests that involve a complete Pinot cluster.
 */
public abstract class ClusterTest extends ControllerTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTest.class);
  private static final int DEFAULT_BROKER_PORT = 18099;
  protected static final Random RANDOM = new Random(System.currentTimeMillis());

  protected String _brokerBaseApiUrl;

  private List<HelixBrokerStarter> _brokerStarters;
  private List<HelixServerStarter> _serverStarters;
  private MinionStarter _minionStarter;
  private List<Integer> _brokerPorts;

  protected PinotConfiguration getDefaultBrokerConfiguration() {
    return new PinotConfiguration();
  }

  protected void startBroker()
      throws Exception {
    startBrokers(1);
  }

  protected void startBroker(int port, String zkStr)
      throws Exception {
    startBrokers(1, port, zkStr);
  }

  protected void startBrokers(int numBrokers)
      throws Exception {
    startBrokers(numBrokers, DEFAULT_BROKER_PORT, getZkUrl());
  }

  protected void startBrokers(int numBrokers, int basePort, String zkStr)
      throws Exception {
    _brokerStarters = new ArrayList<>(numBrokers);
    _brokerPorts = new ArrayList<>();
    for (int i = 0; i < numBrokers; i++) {
      Map<String, Object> properties = getDefaultBrokerConfiguration().toMap();
      properties.put(Broker.CONFIG_OF_BROKER_TIMEOUT_MS, 60 * 1000L);
      int port = NetUtils.findOpenPort(basePort + i);
      _brokerPorts.add(port);
      properties.put(Helix.KEY_OF_BROKER_QUERY_PORT, port);
      properties.put(Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, 0);
      PinotConfiguration configuration = new PinotConfiguration(properties);
      overrideBrokerConf(configuration);

      HelixBrokerStarter brokerStarter =
          new HelixBrokerStarter(configuration, getHelixClusterName(), zkStr, LOCAL_HOST);
      brokerStarter.start();
      _brokerStarters.add(brokerStarter);
    }
    _brokerBaseApiUrl = "http://localhost:" + _brokerPorts.get(0);
  }

  protected int getRandomBrokerPort() {
    return _brokerPorts.get(RANDOM.nextInt(_brokerPorts.size()));
  }

  protected int getBrokerPort(int index) {
    return _brokerPorts.get(index);
  }

  protected List<Integer> getBrokerPorts() {
    return ImmutableList.copyOf(_brokerPorts);
  }

  protected PinotConfiguration getDefaultServerConfiguration() {
    PinotConfiguration configuration = DefaultHelixStarterServerConfig.loadDefaultServerConf();

    configuration.setProperty(Helix.KEY_OF_SERVER_NETTY_HOST, LOCAL_HOST);
    configuration.setProperty(Server.CONFIG_OF_SEGMENT_FORMAT_VERSION, "v3");
    configuration.setProperty(Server.CONFIG_OF_SHUTDOWN_ENABLE_QUERY_CHECK, false);

    return configuration;
  }

  protected void startServer() {
    startServers(1);
  }

  protected void startServer(PinotConfiguration configuration) {
    startServers(1, configuration, Server.DEFAULT_ADMIN_API_PORT, Helix.DEFAULT_SERVER_NETTY_PORT,
        getZkUrl());
  }

  protected void startServers(int numServers) {
    startServers(numServers, getDefaultServerConfiguration(), Server.DEFAULT_ADMIN_API_PORT,
        Helix.DEFAULT_SERVER_NETTY_PORT, getZkUrl());
  }

  protected void startServers(int numServers, int baseAdminApiPort, int baseNettyPort, String zkStr) {
    startServers(numServers, getDefaultServerConfiguration(), baseAdminApiPort, baseNettyPort, zkStr);
  }

  protected void startServers(int numServers, PinotConfiguration configuration, int baseAdminApiPort, int baseNettyPort,
      String zkStr) {
    FileUtils.deleteQuietly(new File(Server.DEFAULT_INSTANCE_BASE_DIR));
    _serverStarters = new ArrayList<>(numServers);
    overrideServerConf(configuration);
    try {
      for (int i = 0; i < numServers; i++) {
        configuration.setProperty(Server.CONFIG_OF_INSTANCE_DATA_DIR, Server.DEFAULT_INSTANCE_DATA_DIR + "-" + i);
        configuration
            .setProperty(Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR, Server.DEFAULT_INSTANCE_SEGMENT_TAR_DIR + "-" + i);
        configuration.setProperty(Server.CONFIG_OF_ADMIN_API_PORT, baseAdminApiPort - i);
        configuration.setProperty(Server.CONFIG_OF_NETTY_PORT, baseNettyPort + i);
        // Thread time measurement is disabled by default, enable it in integration tests.
        // TODO: this can be removed when we eventually enable thread time measurement by default.
        configuration.setProperty(Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
        HelixServerStarter helixServerStarter = new HelixServerStarter(getHelixClusterName(), zkStr, configuration);
        _serverStarters.add(helixServerStarter);
        helixServerStarter.start();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected PinotConfiguration getDefaultMinionConfiguration() {
    return new PinotConfiguration();
  }

  // NOTE: We don't allow multiple Minion instances in the same JVM because Minion uses singleton class MinionContext
  //       to manage the instance level configs
  protected void startMinion(@Nullable List<PinotTaskExecutorFactory> taskExecutorFactories,
      @Nullable List<MinionEventObserverFactory> eventObserverFactories) {
    FileUtils.deleteQuietly(new File(Minion.DEFAULT_INSTANCE_BASE_DIR));
    try {
      _minionStarter = new MinionStarter(getHelixClusterName(), getZkUrl(), getDefaultMinionConfiguration());
      // Register task executor factories
      if (taskExecutorFactories != null) {
        for (PinotTaskExecutorFactory taskExecutorFactory : taskExecutorFactories) {
          _minionStarter.registerTaskExecutorFactory(taskExecutorFactory);
        }
      }
      // Register event observer factories
      if (eventObserverFactories != null) {
        for (MinionEventObserverFactory eventObserverFactory : eventObserverFactories) {
          _minionStarter.registerEventObserverFactory(eventObserverFactory);
        }
      }
      _minionStarter.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void overrideServerConf(PinotConfiguration configuration) {
    // Do nothing, to be overridden by tests if they need something specific
  }

  protected void overrideBrokerConf(PinotConfiguration configuration) {
    // Do nothing, to be overridden by tests if they need something specific
  }

  protected void stopBroker() {
    assertNotNull(_brokerStarters, "Brokers are not started");
    for (HelixBrokerStarter brokerStarter : _brokerStarters) {
      try {
        brokerStarter.stop();
      } catch (Exception e) {
        LOGGER.error("Encountered exception while stopping broker {}", e.getMessage());
      }
    }
    _brokerStarters = null;
  }

  protected void stopServer() {
    assertNotNull(_serverStarters, "Servers are not started");
    for (HelixServerStarter helixServerStarter : _serverStarters) {
      try {
        helixServerStarter.stop();
      } catch (Exception e) {
        LOGGER.error("Encountered exception while stopping server {}", e.getMessage());
      }
    }
    FileUtils.deleteQuietly(new File(Server.DEFAULT_INSTANCE_BASE_DIR));
    _serverStarters = null;
  }

  protected void stopMinion() {
    assertNotNull(_minionStarter, "Minion is not started");
    try {
      _minionStarter.stop();
    } catch (Exception e) {
      LOGGER.error("Encountered exception while stopping minion {}", e.getMessage());
    }
    FileUtils.deleteQuietly(new File(Minion.DEFAULT_INSTANCE_BASE_DIR));
    _minionStarter = null;
  }

  /**
   * Upload all segments inside the given directory to the cluster.
   *
   * @param tarDir Segment directory
   */
  protected void uploadSegments(String tableName, File tarDir)
      throws Exception {
    File[] segmentTarFiles = tarDir.listFiles();
    assertNotNull(segmentTarFiles);
    int numSegments = segmentTarFiles.length;
    assertTrue(numSegments > 0);

    URI uploadSegmentHttpURI = FileUploadDownloadClient.getUploadSegmentHttpURI(LOCAL_HOST, _controllerPort);
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      if (numSegments == 1) {
        File segmentTarFile = segmentTarFiles[0];
        if (System.currentTimeMillis() % 2 == 0) {
          assertEquals(fileUploadDownloadClient
                  .uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(), segmentTarFile, tableName).getStatusCode(),
              HttpStatus.SC_OK);
        } else {
          assertEquals(
              uploadSegmentWithOnlyMetadata(tableName, uploadSegmentHttpURI, fileUploadDownloadClient, segmentTarFile),
              HttpStatus.SC_OK);
        }
      } else {
        // Upload all segments in parallel
        ExecutorService executorService = Executors.newFixedThreadPool(numSegments);
        List<Future<Integer>> futures = new ArrayList<>(numSegments);
        for (File segmentTarFile : segmentTarFiles) {
          futures.add(executorService.submit(() -> {
            if (System.currentTimeMillis() % 2 == 0) {
              return fileUploadDownloadClient
                  .uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(), segmentTarFile, tableName)
                  .getStatusCode();
            } else {
              return uploadSegmentWithOnlyMetadata(tableName, uploadSegmentHttpURI, fileUploadDownloadClient,
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

  private int uploadSegmentWithOnlyMetadata(String tableName, URI uploadSegmentHttpURI,
      FileUploadDownloadClient fileUploadDownloadClient, File segmentTarFile)
      throws IOException, HttpErrorStatusException {
    List<Header> headers = ImmutableList.of(new BasicHeader(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI,
            "file://" + segmentTarFile.getParentFile().getAbsolutePath() + "/" + URLEncoder
                .encode(segmentTarFile.getName(), StandardCharsets.UTF_8.toString())),
        new BasicHeader(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE,
            FileUploadDownloadClient.FileUploadType.METADATA.toString()));
    // Add table name as a request parameter
    NameValuePair tableNameValuePair =
        new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME, tableName);
    List<NameValuePair> parameters = Arrays.asList(tableNameValuePair);
    return fileUploadDownloadClient
        .uploadSegmentMetadata(uploadSegmentHttpURI, segmentTarFile.getName(), segmentTarFile, headers, parameters,
            fileUploadDownloadClient.DEFAULT_SOCKET_TIMEOUT_MS).getStatusCode();
  }

  public static class AvroFileSchemaKafkaAvroMessageDecoder implements StreamMessageDecoder<byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroFileSchemaKafkaAvroMessageDecoder.class);
    public static File avroFile;
    private org.apache.avro.Schema _avroSchema;
    private RecordExtractor _recordExtractor;
    private DecoderFactory _decoderFactory = new DecoderFactory();
    private DatumReader<GenericData.Record> _reader;

    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
        throws Exception {
      // Load Avro schema
      try (DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile)) {
        _avroSchema = reader.getSchema();
      }
      _recordExtractor = new AvroRecordExtractor();
      _recordExtractor.init(fieldsToRead, null);
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
        return _recordExtractor.extract(avroRecord, destination);
      } catch (Exception e) {
        LOGGER.error("Caught exception", e);
        throw new RuntimeException(e);
      }
    }
  }

  protected JsonNode getDebugInfo(final String uri)
      throws Exception {
    return JsonUtils.stringToJsonNode(sendGetRequest(_brokerBaseApiUrl + "/" + uri));
  }

  /**
   * Queries the broker's pql query endpoint (/query)
   */
  protected JsonNode postQuery(String query)
      throws Exception {
    return postQuery(query, _brokerBaseApiUrl);
  }

  /**
   * Queries the broker's pql query endpoint (/query)
   */
  public static JsonNode postQuery(String query, String brokerBaseApiUrl)
      throws Exception {
    return postQuery(query, brokerBaseApiUrl, null);
  }

  /**
   * Queries the broker's pql query endpoint (/query)
   */
  public static JsonNode postQuery(String query, String brokerBaseApiUrl, Map<String, String> headers)
      throws Exception {
    return postQuery(query, brokerBaseApiUrl, false, Broker.Request.PQL, headers);
  }

  /**
   * Queries the broker's pql query endpoint (/query)
   */
  public static JsonNode postQuery(String query, String brokerBaseApiUrl, boolean enableTrace, String queryType)
      throws Exception {
    return postQuery(query, brokerBaseApiUrl, enableTrace, queryType, null);
  }

  /**
   * Queries the broker's pql query endpoint (/query)
   */
  public static JsonNode postQuery(String query, String brokerBaseApiUrl, boolean enableTrace, String queryType, Map<String, String> headers)
      throws Exception {
    ObjectNode payload = JsonUtils.newObjectNode();
    payload.put(queryType, query);
    payload.put("trace", enableTrace);

    return JsonUtils.stringToJsonNode(sendPostRequest(brokerBaseApiUrl + "/query", payload.toString(), headers));
  }

  /**
   * Queries the broker's sql query endpoint (/query/sql)
   */
  protected JsonNode postSqlQuery(String query)
      throws Exception {
    return postSqlQuery(query, _brokerBaseApiUrl);
  }

  /**
   * Queries the broker's sql query endpoint (/sql)
   */
  public static JsonNode postSqlQuery(String query, String brokerBaseApiUrl)
      throws Exception {
    return postSqlQuery(query, brokerBaseApiUrl, null);
  }

  /**
   * Queries the broker's sql query endpoint (/sql)
   */
  public static JsonNode postSqlQuery(String query, String brokerBaseApiUrl, Map<String, String> headers)
      throws Exception {
    ObjectNode payload = JsonUtils.newObjectNode();
    payload.put("sql", query);
    payload.put("queryOptions", "groupByMode=sql;responseFormat=sql");

    return JsonUtils.stringToJsonNode(sendPostRequest(brokerBaseApiUrl + "/query/sql", payload.toString(), headers));
  }
}
