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
package org.apache.pinot.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.streams.AirlineDataStream;
import org.apache.pinot.tools.streams.MeetupRsvpStream;
import org.apache.pinot.tools.streams.RsvpSourceGenerator;
import org.apache.pinot.tools.utils.JarUtils;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.apache.pinot.tools.utils.PinotConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Assuming that database name is DBNAME, bootstrap path must have the file structure specified below to properly
 * load the table:
 *  DBNAME
 *  ├── ingestionJobSpec.yaml
 *  ├── rawdata
 *  │   └── DBNAME_data.csv
 *  ├── DBNAME_offline_table_config.json
 *  └── DBNAME_schema.json
 *
 */
public abstract class QuickStartBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuickStartBase.class);
  private static final String TAB = "\t\t";
  private static final String NEW_LINE = "\n";

  protected static final String[] DEFAULT_OFFLINE_TABLE_DIRECTORIES = new String[]{
      "examples/batch/airlineStats",
      "examples/minions/batch/baseballStats",
      "examples/batch/dimBaseballTeams",
      "examples/batch/starbucksStores",
      "examples/batch/githubEvents",
      "examples/batch/githubComplexTypeEvents",
      "examples/batch/billing",
      "examples/batch/fineFoodReviews",
  };

  protected static final Map<String, String> DEFAULT_STREAM_TABLE_DIRECTORIES = ImmutableMap.<String, String>builder()
      .put("airlineStats", "examples/stream/airlineStats")
      .put("githubEvents", "examples/stream/githubEvents")
      .put("meetupRsvp", "examples/stream/meetupRsvp")
      .put("meetupRsvpJson", "examples/stream/meetupRsvpJson")
      .put("meetupRsvpComplexType", "examples/stream/meetupRsvpComplexType")
      .put("upsertMeetupRsvp", "examples/stream/upsertMeetupRsvp")
      .put("upsertJsonMeetupRsvp", "examples/stream/upsertJsonMeetupRsvp")
      .put("upsertPartialMeetupRsvp", "examples/stream/upsertPartialMeetupRsvp")
      .build();

  protected File _dataDir = FileUtils.getTempDirectory();
  protected String[] _bootstrapDataDirs;
  protected String _zkExternalAddress;
  protected String _configFilePath;
  protected StreamDataServerStartable _kafkaStarter;
  protected ZkStarter.ZookeeperInstance _zookeeperInstance;

  public QuickStartBase setDataDir(String dataDir) {
    _dataDir = new File(dataDir);
    return this;
  }

  public QuickStartBase setBootstrapDataDirs(String[] bootstrapDataDirs) {
    _bootstrapDataDirs = bootstrapDataDirs;
    return this;
  }

  /* @return bootstrap path if specified by command line argument -bootstrapTableDir; otherwise, null. */
  public String getBootstrapDataDir() {
    return _bootstrapDataDirs != null && _bootstrapDataDirs.length == 1 ? _bootstrapDataDirs[0] : null;
  }

  /** @return Table name specified by command line argument -bootstrapTableDir */
  public String getTableName() {
    return Paths.get(getBootstrapDataDir()).getFileName().toString();
  }

  /** @return Table name if specified by input bootstrap directory. */
  public String getTableName(String bootstrapDataDir) {
    return Paths.get(bootstrapDataDir).getFileName().toString();
  }

  /** @return true if bootstrapTableDir is not specified by command line argument -bootstrapTableDir, else false.*/
  public boolean useDefaultBootstrapTableDir() {
    return _bootstrapDataDirs == null;
  }

  public QuickStartBase setZkExternalAddress(String zkExternalAddress) {
    _zkExternalAddress = zkExternalAddress;
    return this;
  }

  public QuickStartBase setConfigFilePath(String configFilePath) {
    _configFilePath = configFilePath;
    return this;
  }

  public abstract List<String> types();

  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
  }

  protected void waitForBootstrapToComplete(QuickstartRunner runner)
      throws Exception {
    QuickStartBase.printStatus(Quickstart.Color.CYAN,
        "***** Waiting for 5 seconds for the server to fetch the assigned segment *****");
    Thread.sleep(5000);
  }

  public static void printStatus(Quickstart.Color color, String message) {
    System.out.println(color.getCode() + message + Quickstart.Color.RESET.getCode());
  }

  public abstract void execute()
      throws Exception;

  protected List<QuickstartTableRequest> bootstrapOfflineTableDirectories(File quickstartTmpDir)
      throws IOException {
    List<QuickstartTableRequest> quickstartTableRequests = new ArrayList<>();
    if (useDefaultBootstrapTableDir()) {
      for (String directory : getDefaultBatchTableDirectories()) {
        String tableName = getTableName(directory);
        File baseDir = new File(quickstartTmpDir, tableName);
        File dataDir = new File(baseDir, "rawdata");
        Preconditions.checkState(dataDir.mkdirs());
        copyResourceTableToTmpDirectory(directory, tableName, baseDir, dataDir, false);
        quickstartTableRequests.add(new QuickstartTableRequest(baseDir.getAbsolutePath()));
      }
    } else {
      String tableName = getTableName();
      File baseDir = new File(quickstartTmpDir, tableName);
      copyFilesystemTableToTmpDirectory(getBootstrapDataDir(), tableName, baseDir);
      quickstartTableRequests.add(new QuickstartTableRequest(baseDir.getAbsolutePath()));
    }
    return quickstartTableRequests;
  }

  private static void copyResourceTableToTmpDirectory(String sourcePath, String tableName, File baseDir, File dataDir,
      boolean isStreamTable)
      throws IOException {
    ClassLoader classLoader = Quickstart.class.getClassLoader();
    // Copy schema
    URL resource = classLoader.getResource(sourcePath + File.separator + tableName + "_schema.json");
    Preconditions.checkNotNull(resource, "Missing schema json file for table - " + tableName);
    File schemaFile = new File(baseDir, tableName + "_schema.json");
    FileUtils.copyURLToFile(resource, schemaFile);

    // Copy table config
    String tableConfigFileSuffix = isStreamTable ? "_realtime_table_config.json" : "_offline_table_config.json";
    File tableConfigFile = new File(baseDir, tableName + tableConfigFileSuffix);
    String sourceTableConfig = sourcePath + File.separator + tableName + tableConfigFileSuffix;
    resource = classLoader.getResource(sourceTableConfig);
    Preconditions.checkNotNull(resource, "Missing table config file for table - " + tableName);
    FileUtils.copyURLToFile(resource, tableConfigFile);

    // Copy raw data
    String sourceRawDataPath = sourcePath + File.separator + "rawdata";
    resource = classLoader.getResource(sourceRawDataPath);
    if (resource != null) {
      File rawDataDir = new File(resource.getFile());
      if (rawDataDir.isDirectory()) {
        // Copy the directory from `pinot-tools/src/main/resources/examples` directory. This code path is used for
        // running Quickstart inside IDE, `ClassLoader.getResource()` should source it at build directory,
        // e.g. `/pinot-tools/target/classes/examples/batch/airlineStats/rawdata`
        FileUtils.copyDirectory(rawDataDir, dataDir);
      } else {
        // Copy the directory recursively from a jar file. This code path is used for running Quickstart using
        // pinot-admin script. The `ClassLoader.getResource()` should found the resources in the jar file then
        // decompress it, e.g. `lib/pinot-all-jar-with-dependencies.jar!/examples/batch/airlineStats/rawdata`
        String[] jarPathSplits = resource.toString().split("!/", 2);
        JarUtils.copyResourcesToDirectory(jarPathSplits[0], jarPathSplits[1], dataDir.getAbsolutePath());
      }
    } else {
      LOGGER.warn("Not found rawdata directory for table {} from {}", tableName, sourceRawDataPath);
    }

    if (!isStreamTable) {
      // Copy ingestion job spec file
      resource = classLoader.getResource(sourcePath + File.separator + "ingestionJobSpec.yaml");
      if (resource != null) {
        File ingestionJobSpecFile = new File(baseDir, "ingestionJobSpec.yaml");
        FileUtils.copyURLToFile(resource, ingestionJobSpecFile);
      }
    }
  }

  protected List<QuickstartTableRequest> bootstrapStreamTableDirectories(File quickstartTmpDir)
      throws IOException {
    List<QuickstartTableRequest> quickstartTableRequests = new ArrayList<>();
    for (Map.Entry<String, String> entry : getDefaultStreamTableDirectories().entrySet()) {
      String tableName = entry.getKey();
      String directory = entry.getValue();
      File baseDir = new File(quickstartTmpDir, tableName);
      File dataDir = new File(baseDir, "rawdata");
      dataDir.mkdirs();
      if (useDefaultBootstrapTableDir()) {
        copyResourceTableToTmpDirectory(directory, tableName, baseDir, dataDir, true);
      } else {
        copyFilesystemTableToTmpDirectory(directory, tableName, baseDir);
      }
      quickstartTableRequests.add(new QuickstartTableRequest(baseDir.getAbsolutePath()));
    }
    return quickstartTableRequests;
  }

  private static void copyFilesystemTableToTmpDirectory(String sourcePath, String tableName, File baseDir)
      throws IOException {
    File fileDb = new File(sourcePath);

    if (!fileDb.exists() || !fileDb.isDirectory()) {
      throw new RuntimeException("Directory " + fileDb.getAbsolutePath() + " not found.");
    }

    File schemaFile = new File(fileDb, tableName + "_schema.json");
    if (!schemaFile.exists()) {
      throw new RuntimeException("Schema file " + schemaFile.getAbsolutePath() + " not found.");
    }

    File tableFile = new File(fileDb, tableName + "_offline_table_config.json");
    if (!tableFile.exists()) {
      throw new RuntimeException("Table table " + tableFile.getAbsolutePath() + " not found.");
    }

    File data = new File(fileDb, "rawdata" + File.separator + tableName + "_data.csv");
    if (!data.exists()) {
      throw new RuntimeException(("Data file " + data.getAbsolutePath() + " not found. "));
    }

    FileUtils.copyDirectory(fileDb, baseDir);
  }

  protected Map<String, Object> getConfigOverrides() {
    try {
      return StringUtils.isEmpty(_configFilePath) ? ImmutableMap.of()
          : PinotConfigUtils.readConfigFromFile(_configFilePath);
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  protected String[] getDefaultBatchTableDirectories() {
    return DEFAULT_OFFLINE_TABLE_DIRECTORIES;
  }

  public static String prettyPrintResponse(JsonNode response) {
    StringBuilder responseBuilder = new StringBuilder();

    // Sql Results
    if (response.has("resultTable")) {
      JsonNode columns = response.get("resultTable").get("dataSchema").get("columnNames");
      int numColumns = columns.size();
      for (int i = 0; i < numColumns; i++) {
        responseBuilder.append(jsonNode2String(columns.get(i))).append(TAB);
      }
      responseBuilder.append(NEW_LINE);
      JsonNode rows = response.get("resultTable").get("rows");
      for (int i = 0; i < rows.size(); i++) {
        JsonNode row = rows.get(i);
        for (int j = 0; j < numColumns; j++) {
          responseBuilder.append(jsonNode2String(row.get(j))).append(TAB);
        }
        responseBuilder.append(NEW_LINE);
      }
    }
    return responseBuilder.toString();
  }

  private static String jsonNode2String(JsonNode jsonNode) {
    if (jsonNode instanceof ArrayNode) {
      ArrayNode arrayNode = (ArrayNode) jsonNode;
      String result = "[";
      for (int i = 0; i < arrayNode.size() - 1; i++) {
        result += jsonNode2String(arrayNode.get(i)) + ", ";
      }
      if (arrayNode.size() > 0) {
        result += jsonNode2String(arrayNode.get(arrayNode.size() - 1));
      }
      result += "]";
      return result;
    }
    return jsonNode.asText();
  }

  protected Map<String, String> getDefaultStreamTableDirectories() {
    return DEFAULT_STREAM_TABLE_DIRECTORIES;
  }

  protected static void publishStreamDataToKafka(String tableName, File dataDir)
      throws Exception {
    switch (tableName) {
      case "githubEvents":
        publishGithubEventsToKafka("githubEvents", new File(dataDir, "/rawdata/2021-07-21-few-hours.json"));
        break;
      default:
        break;
    }
  }

  protected static void publishGithubEventsToKafka(String topicName, File dataFile)
      throws Exception {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    StreamDataProducer producer =
        StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, properties);
    try {
      LineIterator dataStream = FileUtils.lineIterator(dataFile);

      while (dataStream.hasNext()) {
        producer.produce(topicName, dataStream.nextLine().getBytes(StandardCharsets.UTF_8));
      }
    } finally {
      producer.close();
    }
  }

  protected void startKafka() {
    printStatus(Quickstart.Color.CYAN, "***** Starting Kafka *****");
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    try {
      _kafkaStarter = StreamDataProvider.getServerDataStartable(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME,
          KafkaStarterUtils.getDefaultKafkaConfiguration(_zookeeperInstance));
    } catch (Exception e) {
      throw new RuntimeException("Failed to start " + KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, e);
    }
    _kafkaStarter.start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Quickstart.Color.GREEN, "***** Shutting down kafka and zookeeper *****");
        _kafkaStarter.stop();
        ZkStarter.stopLocalZkServer(_zookeeperInstance);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));

    printStatus(Quickstart.Color.CYAN, "***** Kafka Started *****");
  }

  public void startAllDataStreams(StreamDataServerStartable kafkaStarter, File quickstartTmpDir)
      throws Exception {
    for (String streamName : getDefaultStreamTableDirectories().keySet()) {
      switch (streamName) {
        case "airlineStats":
          kafkaStarter.createTopic("flights-realtime", KafkaStarterUtils.getTopicCreationProps(10));
          printStatus(Quickstart.Color.CYAN, "***** Starting airlineStats data stream and publishing to Kafka *****");
          AirlineDataStream airlineDataStream = new AirlineDataStream(new File(quickstartTmpDir, "airlineStats"));
          airlineDataStream.run();
          Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
              airlineDataStream.shutdown();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }));
          break;
        case "meetupRsvp":
          kafkaStarter.createTopic("meetupRSVPEvents", KafkaStarterUtils.getTopicCreationProps(10));
          printStatus(Quickstart.Color.CYAN, "***** Starting meetup data stream and publishing to Kafka *****");
          MeetupRsvpStream meetupRSVPProvider = new MeetupRsvpStream(true);
          meetupRSVPProvider.run();
          Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
              meetupRSVPProvider.stopPublishing();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }));
          break;
        case "meetupRsvpJson":
          kafkaStarter.createTopic("meetupRSVPJsonEvents", KafkaStarterUtils.getTopicCreationProps(10));
          printStatus(Quickstart.Color.CYAN, "***** Starting meetupRsvpJson data stream and publishing to Kafka *****");
          MeetupRsvpStream meetupRSVPJsonProvider = new MeetupRsvpStream("meetupRSVPJsonEvents");
          meetupRSVPJsonProvider.run();
          Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
              meetupRSVPJsonProvider.stopPublishing();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }));
          break;
        case "meetupRsvpComplexType":
          kafkaStarter.createTopic("meetupRSVPComplexTypeEvents", KafkaStarterUtils.getTopicCreationProps(10));
          printStatus(Quickstart.Color.CYAN,
              "***** Starting meetupRSVPComplexType data stream and publishing to Kafka *****");
          MeetupRsvpStream meetupRSVPComplexTypeProvider = new MeetupRsvpStream("meetupRSVPComplexTypeEvents");
          meetupRSVPComplexTypeProvider.run();
          Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
              meetupRSVPComplexTypeProvider.stopPublishing();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }));
          break;
        case "upsertMeetupRsvp":
          kafkaStarter.createTopic("upsertMeetupRSVPEvents", KafkaStarterUtils.getTopicCreationProps(2));
          printStatus(Quickstart.Color.CYAN,
              "***** Starting upsertMeetupRSVPEvents data stream and publishing to Kafka *****");
          MeetupRsvpStream upsertMeetupRsvpProvider =
              new MeetupRsvpStream("upsertMeetupRSVPEvents", RsvpSourceGenerator.KeyColumn.EVENT_ID);
          upsertMeetupRsvpProvider.run();
          Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
              upsertMeetupRsvpProvider.stopPublishing();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }));
          break;
        case "upsertJsonMeetupRsvp":
          kafkaStarter.createTopic("upsertJsonMeetupRSVPEvents", KafkaStarterUtils.getTopicCreationProps(2));
          printStatus(Quickstart.Color.CYAN,
              "***** Starting upsertJsonMeetupRSVPEvents data stream and publishing to Kafka *****");
          MeetupRsvpStream upsertJsonMeetupRsvpProvider =
              new MeetupRsvpStream("upsertJsonMeetupRSVPEvents", RsvpSourceGenerator.KeyColumn.RSVP_ID);
          upsertJsonMeetupRsvpProvider.run();
          Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
              upsertJsonMeetupRsvpProvider.stopPublishing();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }));
          break;
        case "upsertPartialMeetupRsvp":
          kafkaStarter.createTopic("upsertPartialMeetupRSVPEvents", KafkaStarterUtils.getTopicCreationProps(2));
          printStatus(Quickstart.Color.CYAN,
              "***** Starting upsertPartialMeetupRSVPEvents data stream and publishing to Kafka *****");
          MeetupRsvpStream upsertPartialMeetupRsvpProvider =
              new MeetupRsvpStream("upsertPartialMeetupRSVPEvents", RsvpSourceGenerator.KeyColumn.EVENT_ID);
          upsertPartialMeetupRsvpProvider.run();
          Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
              upsertPartialMeetupRsvpProvider.stopPublishing();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }));
          break;
        case "githubEvents":
          kafkaStarter.createTopic("githubEvents", KafkaStarterUtils.getTopicCreationProps(2));
          printStatus(Quickstart.Color.CYAN, "***** Starting githubEvents data stream and publishing to Kafka *****");
          publishStreamDataToKafka("githubEvents", new File(quickstartTmpDir, "githubEvents"));
          break;
        default:
          throw new UnsupportedOperationException("Unknown stream name: " + streamName);
      }
    }
  }
}
