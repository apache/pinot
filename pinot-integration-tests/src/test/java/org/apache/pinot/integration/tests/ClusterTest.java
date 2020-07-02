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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.broker.requesthandler.PinotQueryRequest;
import org.apache.pinot.common.utils.CommonConstants.Broker;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.common.utils.CommonConstants.Minion;
import org.apache.pinot.common.utils.CommonConstants.Server;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.minion.MinionStarter;
import org.apache.pinot.minion.events.MinionEventObserverFactory;
import org.apache.pinot.minion.executor.PinotTaskExecutorFactory;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.server.starter.helix.DefaultHelixStarterServerConfig;
import org.apache.pinot.server.starter.helix.HelixServerStarter;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.JsonUtils;
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

  protected String _brokerBaseApiUrl;

  private List<HelixBrokerStarter> _brokerStarters;
  private List<HelixServerStarter> _serverStarters;
  private MinionStarter _minionStarter;

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
    startBrokers(numBrokers, DEFAULT_BROKER_PORT, ZkStarter.DEFAULT_ZK_STR);
  }

  protected void startBrokers(int numBrokers, int basePort, String zkStr)
      throws Exception {
    _brokerBaseApiUrl = "http://localhost:" + basePort;
    _brokerStarters = new ArrayList<>(numBrokers);
    for (int i = 0; i < numBrokers; i++) {
      Configuration brokerConf = new BaseConfiguration();
      brokerConf.setProperty(Broker.CONFIG_OF_BROKER_TIMEOUT_MS, 60 * 1000L);
      brokerConf.setProperty(Helix.KEY_OF_BROKER_QUERY_PORT, Integer.toString(basePort + i));
      brokerConf.setProperty(Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, 0);
      overrideBrokerConf(brokerConf);
      HelixBrokerStarter brokerStarter = new HelixBrokerStarter(brokerConf, getHelixClusterName(), zkStr, LOCAL_HOST);
      brokerStarter.start();
      _brokerStarters.add(brokerStarter);
    }
  }

  public static Configuration getDefaultServerConfiguration() {
    Configuration configuration = DefaultHelixStarterServerConfig.loadDefaultServerConf();
    configuration.setProperty(Helix.KEY_OF_SERVER_NETTY_HOST, LOCAL_HOST);
    configuration.setProperty(Server.CONFIG_OF_SEGMENT_FORMAT_VERSION, "v3");
    configuration.setProperty(Server.CONFIG_OF_SHUTDOWN_ENABLE_QUERY_CHECK, false);
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
        _serverStarters.add(new HelixServerStarter(getHelixClusterName(), zkStr, configuration));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // NOTE: We don't allow multiple Minion instances in the same JVM because Minion uses singleton class MinionContext
  //       to manage the instance level configs
  protected void startMinion(@Nullable Map<String, PinotTaskExecutorFactory> taskExecutorFactoryRegistry,
      @Nullable Map<String, MinionEventObserverFactory> eventObserverFactoryRegistry) {
    FileUtils.deleteQuietly(new File(Minion.DEFAULT_INSTANCE_BASE_DIR));
    try {
      _minionStarter =
          new MinionStarter(ZkStarter.DEFAULT_ZK_STR, getHelixClusterName(), new PropertiesConfiguration());
      // Register task executor factories
      if (taskExecutorFactoryRegistry != null) {
        for (Map.Entry<String, PinotTaskExecutorFactory> entry : taskExecutorFactoryRegistry.entrySet()) {
          _minionStarter.registerTaskExecutorFactory(entry.getKey(), entry.getValue());
        }
      }
      // Register event observer factories
      if (eventObserverFactoryRegistry != null) {
        for (Map.Entry<String, MinionEventObserverFactory> entry : eventObserverFactoryRegistry.entrySet()) {
          _minionStarter.registerEventObserverFactory(entry.getKey(), entry.getValue());
        }
      }
      _minionStarter.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void overrideServerConf(Configuration configuration) {
    // Do nothing, to be overridden by tests if they need something specific
  }

  protected void overrideBrokerConf(Configuration brokerConf) {
    // Do nothing, to be overridden by tests if they need something specific
  }

  protected void stopBroker() {
    assertNotNull(_brokerStarters, "Brokers are not started");
    for (HelixBrokerStarter brokerStarter : _brokerStarters) {
      try {
        brokerStarter.shutdown();
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
        assertEquals(fileUploadDownloadClient
                .uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(), segmentTarFile, tableName).getStatusCode(),
            HttpStatus.SC_OK);
      } else {
        // Upload all segments in parallel
        ExecutorService executorService = Executors.newFixedThreadPool(numSegments);
        List<Future<Integer>> futures = new ArrayList<>(numSegments);
        for (File segmentTarFile : segmentTarFiles) {
          futures.add(executorService.submit(() -> fileUploadDownloadClient
              .uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(), segmentTarFile, tableName)
              .getStatusCode()));
        }
        executorService.shutdown();
        for (Future<Integer> future : futures) {
          assertEquals((int) future.get(), HttpStatus.SC_OK);
        }
      }
    }
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
    return postQuery(new PinotQueryRequest("pql", query), _brokerBaseApiUrl);
  }

  /**
   * Queries the broker's pql query endpoint (/query)
   */
  public static JsonNode postQuery(PinotQueryRequest r, String brokerBaseApiUrl)
      throws Exception {
    return postQuery(r.getQuery(), brokerBaseApiUrl, false, r.getQueryFormat());
  }

  /**
   * Queries the broker's pql query endpoint (/query)
   */
  public static JsonNode postQuery(String query, String brokerBaseApiUrl, boolean enableTrace, String queryType)
      throws Exception {
    ObjectNode payload = JsonUtils.newObjectNode();
    payload.put(queryType, query);
    payload.put("trace", enableTrace);

    return JsonUtils.stringToJsonNode(sendPostRequest(brokerBaseApiUrl + "/query", payload.toString()));
  }

  /**
   * Queries the broker's sql query endpoint (/sql)
   */
  static JsonNode postSqlQuery(String query, String brokerBaseApiUrl)
      throws Exception {
    ObjectNode payload = JsonUtils.newObjectNode();
    payload.put("sql", query);
    payload.put("queryOptions", "groupByMode=sql;responseFormat=sql");

    return JsonUtils.stringToJsonNode(sendPostRequest(brokerBaseApiUrl + "/query/sql", payload.toString()));
  }
}
