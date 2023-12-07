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
      .put("fineFoodReviews", "examples/stream/fineFoodReviews")
      .build();

  protected File _dataDir = FileUtils.getTempDirectory();
  protected boolean _setCustomDataDir;
  protected String[] _bootstrapDataDirs;
  protected String _zkExternalAddress;
  protected String _configFilePath;
  protected StreamDataServerStartable _kafkaStarter;
  protected ZkStarter.ZookeeperInstance _zookeeperInstance;

  public QuickStartBase setDataDir(String dataDir) {
    _dataDir = new File(dataDir);
    _setCustomDataDir = true;
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
        publishLineSplitFileToKafka("githubEvents", new File(dataDir, "/rawdata/2021-07-21-few-hours.json"));
        break;
      case "fineFoodReviews":
        publishLineSplitFileToKafka("fineFoodReviews",
            new File(dataDir, "/rawdata/fine_food_reviews_with_embeddings_1k.json"));
        break;
      default:
        break;
    }
  }

  protected static void publishLineSplitFileToKafka(String topicName, File dataFile)
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
        case "fineFoodReviews":
          kafkaStarter.createTopic("fineFoodReviews", KafkaStarterUtils.getTopicCreationProps(2));
          printStatus(Quickstart.Color.CYAN,
              "***** Starting fineFoodReviews data stream and publishing to Kafka *****");
          publishStreamDataToKafka("fineFoodReviews", new File(quickstartTmpDir, "fineFoodReviews"));
          break;
        default:
          throw new UnsupportedOperationException("Unknown stream name: " + streamName);
      }
    }
  }

  protected void runVectorQueryExamples(QuickstartRunner runner)
      throws Exception {
    // The following embedding is the text: "tomato soup" generated by model "text-embedding-ada-002" from OpenAI.
    // The Python code snippet is:
    //    import openai
    //    from openai.embeddings_utils import get_embedding
    //    openai.api_key = "sk-xxxx"
    //    response = openai.Embedding.create(
    //      model="text-embedding-ada-002",  # You can choose different models
    //      input="tomato soup"
    //    )
    //    search_embedding = response['data'][0]['embedding']
    //    print(search_embedding)
    String vectorArrayLiteral =
        "ARRAY[0.010218413546681404, -0.015945643186569214, -0.008374051190912724, -0.011700374074280262, -0"
            + ".009357711300253868, 0.012360461987555027, -0.022675946354866028, -0.01687753200531006, -0"
            + ".009700697846710682, -0.030778197571635246, 0.023672549054026604, 0.009280053898692131, -0"
            + ".02050154097378254, 0.008736452087759972, -0.02531629614531994, 0.02303834818303585, 0"
            + ".02785310335457325, -0.007241548039019108, 0.0252386387437582, -0.033392660319805145, 0"
            + ".009545383043587208, 0.007493934594094753, 0.016916360706090927, -0.018482450395822525, -0"
            + ".011881574988365173, -0.012748748995363712, 0.0005990132340230048, -0.0218864306807518, 0"
            + ".009584211744368076, -0.0014229092048481107, 0.01646335795521736, -0.022417088970541954, -0"
            + ".011389745399355888, -0.017201103270053864, 0.02022974006831646, 0.01375829428434372, -0"
            + ".010710243135690689, -0.0032632267102599144, 0.022404145449399948, -0.020113253965973854, 0"
            + ".010425499640405178, 0.003106294199824333, -0.001236855168826878, -0.01876719295978546, -0"
            + ".03303026035428047, -0.007804563734680414, 0.008069893345236778, -0.011150301434099674, 0"
            + ".0042679188773036, -0.009642454795539379, 0.03919107839465141, -0.0018686300609260798, -0"
            + ".0029153863433748484, -0.0007668663747608662, 0.006801489740610123, 0.01792590506374836, -0"
            + ".015816213563084602, 0.0005670604878105223, -0.01019899919629097, -0.004623848013579845, 0"
            + ".01546675618737936, 0.01832713559269905, -0.034712836146354675, 0.0126193193718791, -0"
            + ".006685003638267517, -0.014288952574133873, -0.010846143588423729, -0.013913609087467194, 0"
            + ".004083482548594475, 0.008386993780732155, 0.03939816355705261, 0.03409157693386078, 0"
            + ".007739849388599396, -0.00590519467368722, 0.007422748487442732, 0.0013323089806362987, -0"
            + ".012004532851278782, -0.003077172674238682, -0.004138489719480276, -0.01057434268295765, 0"
            + ".023594891652464867, -0.036576613783836365, -0.005892251618206501, 0.014327781274914742, 0"
            + ".02456560917198658, 0.004869763273745775, -0.019116651266813278, 0.03924284875392914, -0"
            + ".006503803189843893, -0.01236693374812603, -0.012036889791488647, 0.011169715784490108, 0"
            + ".026895329356193542, 0.022274717688560486, -0.01748584769666195, 0.018831908702850342, 0"
            + ".007370977196842432, 0.03442809358239174, -0.0034007448703050613, -0.03771558776497841, -0"
            + ".012108075432479382, -0.010677886195480824, 0.0013954055029898882, -0.012392818927764893, -0"
            + ".013201749883592129, -0.021304000169038773, 0.0320466011762619, 0.01057434268295765, -0"
            + ".0013776090927422047, -0.0093706538900733, -0.010697300545871258, 0.032745517790317535, -0"
            + ".010393142700195312, -0.03331500291824341, 0.013667694292962551, -0.009778355248272419, 0"
            + ".012444591149687767, -0.030208710581064224, -0.024267923086881638, 0.0015814596554264426, 0"
            + ".0012069246731698513, 0.006659117992967367, 0.031632427126169205, -0.023504292592406273, 0"
            + ".011939818039536476, -0.00034339111880399287, -0.0053551215678453445, 0.0013816537102684379, -0"
            + ".028370819985866547, -0.010956157930195332, 0.055343806743621826, 0.01859893649816513, 0"
            + ".01748584769666195, -0.0018977515865117311, -0.03279728814959526, 0.018391849473118782, -0"
            + ".03269374370574951, -0.0241902656853199, -0.027594245970249176, -0.02551043964922428, 0"
            + ".0007822360494174063, 0.02676590159535408, 0.0021177807357162237, 0.0027891932986676693, -0"
            + ".006186702288687229, -0.001595211448147893, 0.031114712357521057, 0.0033198518212884665, -0"
            + ".0068597327917814255, 0.00025016182917170227, 0.0053810072131454945, -0.031321797519922256, 0"
            + ".014987869188189507, -0.010237827897071838, -0.0005529041518457234, -0.00018635742890182883, 0"
            + ".010004855692386627, 0.013926551677286625, -0.009066496044397354, 0.011273259297013283, -0"
            + ".01053551398217678, -0.004853584337979555, 0.0063873170875012875, -0.018793080002069473, -0"
            + ".006125223822891712, 0.03398803621530533, 0.02818961814045906, 0.018275363370776176, -0"
            + ".011855688877403736, -0.018145935609936714, 0.013861837796866894, 0.03445398062467575, -0"
            + ".024345580488443375, 0.01409480907022953, -0.013848894275724888, 0.013848894275724888, 0"
            + ".0012360461987555027, 0.0056398650631308556, -0.004358518868684769, -0.018443621695041656, -0"
            + ".04133959859609604, 0.008147550746798515, 0.008852938190102577, 0.044575318694114685, -0"
            + ".010050156153738499, -0.018922507762908936, -6.577618478331715e-05, -0.0027228610124439, -0"
            + ".022857148200273514, -0.016088014468550682, 0.019867340102791786, 0.024889182299375534, 0"
            + ".02171817235648632, -0.004326161462813616, -0.6846272349357605, -0.035489410161972046, -0"
            + ".023840807378292084, -0.025562211871147156, 0.021562857553362846, 0.011713317595422268, -0"
            + ".0012514159316197038, -0.016476301476359367, -0.024591494351625443, -0.01519495528191328, -0"
            + ".013538265600800514, -0.002554603386670351, 0.004112604074180126, -0.00669147539883852, 0"
            + ".013460608199238777, -0.0364471860229969, 0.004009061027318239, -0.007383919786661863, -0"
            + ".012133961543440819, 0.03523055091500282, -0.027335388585925102, 0.0014496039366349578, -0"
            + ".004788869991898537, -0.0027228610124439, 0.02551043964922428, -0.0047791628167033195, 0"
            + ".0036725455429404974, -0.012832877226173878, -0.012910534627735615, 0.006526453420519829, -0"
            + ".023051291704177856, 0.008704095147550106, 0.02548455446958542, -0.007694549392908812, 0"
            + ".057518213987350464, 0.013577093370258808, -0.02079922705888748, 0.027956647798419, 0"
            + ".02381492219865322, 0.03194305673241615, -0.014211295172572136, -0.012729334644973278, 0"
            + ".017602333799004555, -0.006235238164663315, -0.016657501459121704, -0.0005674649146385491, 0"
            + ".0196861382573843, 0.01102734450250864, -0.005704579874873161, -0.022067630663514137, 0"
            + ".035592954605817795, 0.006021680776029825, -0.020410940051078796, 0.0018330371240153909, 0"
            + ".022442974150180817, 0.017226990312337875, 0.014392496086657047, -0.023569006472826004, -0"
            + ".023504292592406273, 0.012897592037916183, -0.00891118124127388, 0.01667044498026371, -0"
            + ".018003562465310097, -0.041831426322460175, 0.015673842281103134, 0.0109949866309762, -0"
            + ".009170038625597954, 0.014081866480410099, 0.01890956610441208, -0.015622070990502834, 0"
            + ".020915713161230087, 0.026921216398477554, 0.0017036081990227103, -0.012399290688335896, 0"
            + ".028552019968628883, 0.016748102381825447, -0.0013193660415709019, 0.011868632398545742, 0"
            + ".008891766890883446, -0.006028152070939541, 0.01173920277506113, -0.005186864174902439, -0"
            + ".003462223568931222, -0.013874780386686325, 0.05881250277161598, -0.00857466645538807, -0"
            + ".03919107839465141, 0.006057273596525192, 0.00021193985594436526, -0.010134284384548664, -0"
            + ".006270831450819969, 0.022753603756427765, -0.025665754452347755, -0.022857148200273514, -0"
            + ".02406083606183529, 0.0010791136883199215, -0.02531629614531994, 0.005093027837574482, 0"
            + ".006002266425639391, -0.03365151956677437, -0.012211618945002556, 0.002769778948277235, 0"
            + ".019608480855822563, 0.0037275529466569424, 0.006519982125610113, 0.011111472733318806, -0"
            + ".008820581249892712, 0.019090766087174416, 0.015039640478789806, -0.03880279138684273, -0"
            + ".0010427117813378572, 0.0033295589964836836, 0.009558325633406639, -0.03784501552581787, 0"
            + ".017602333799004555, -0.013357064686715603, 0.019375508651137352, -0.00658146059140563, 0"
            + ".008755866438150406, -0.021912315860390663, -0.0010718333069235086, -0.0019252551719546318, 0"
            + ".007578063290566206, 0.017356418073177338, 0.008477594703435898, 0.01047080010175705, 8"
            + ".66567061166279e-05, -0.02395729348063469, -0.020177969709038734, -0.02446206659078598, -0"
            + ".0017909726593643427, 0.007442162837833166, -0.0020045305136591196, -0.01721404679119587, 0"
            + ".005044492427259684, 0.004730626940727234, 0.018107106909155846, -0.014224238693714142, 0"
            + ".0209416002035141, 0.0018831908237189054, -0.0025028318632394075, -0.0024009065236896276, -0"
            + ".006545867770910263, 0.0210451427847147, 0.005093027837574482, -0.025199810042977333, -0"
            + ".014405438676476479, -0.015026697888970375, -0.012476948089897633, 0.0077980924397706985, 0"
            + ".010496685281395912, -0.017796477302908897, -0.0037728529423475266, -0.005571914836764336, 0"
            + ".019776739180088043, -0.014185409992933273, 0.00698916194960475, -0.020436827093362808, -0"
            + ".006086395122110844, -0.0017214046092703938, 0.002803754061460495, 0.027464816346764565, -0"
            + ".0019721731077879667, 0.006510274950414896, -0.011383273638784885, -0.028344932943582535, -0"
            + ".015842100605368614, 0.009318882599473, -0.011519174091517925, -0.02456560917198658, 0"
            + ".010024270042777061, -0.02327132038772106, 0.0037178457714617252, 0.01941433735191822, -0"
            + ".02619641274213791, 0.025937555357813835, -0.008445236831903458, -0.00918298214673996, 0"
            + ".032305460423231125, 0.00472739152610302, 0.0014568843180313706, -0.00973952654749155, -0"
            + ".00973952654749155, -0.0014528395840898156, 0.019634367898106575, -7.376437133643776e-05, -0"
            + ".0024834175128489733, 0.016489244997501373, -0.01463841088116169, 0.0376897007226944, -0"
            + ".018987223505973816, -0.004083482548594475, 0.007849863730370998, 0.015635013580322266, 0"
            + ".02531629614531994, -0.019323738291859627, -0.01067141443490982, 0.022274717688560486, 0"
            + ".021744059398770332, -0.008607023395597935, 0.012580491602420807, -0.007474520243704319, 0"
            + ".019802624359726906, -0.010826729238033295, 0.016916360706090927, -0.014250123873353004, 0"
            + ".004937713500112295, 0.0015806506853550673, 0.010988515801727772, 0.007662191987037659, 0"
            + ".006659117992967367, -0.021459314972162247, -0.027438931167125702, -0.002679178724065423, 0"
            + ".0036240098997950554, 0.02852613478899002, 0.013499436900019646, -0.00530982157215476, -0"
            + ".016049185767769814, 0.0013711376814171672, -0.004565604962408543, 0.0009828509064391255, -0"
            + ".005264521576464176, -0.008160493336617947, -0.0196861382573843, 0.0004695843090303242, 0"
            + ".012341047637164593, 0.03435043618083, -0.0004930432769469917, -0.020177969709038734, -0"
            + ".022753603756427765, 0.014327781274914742, 0.01985439658164978, 0.00773337809368968, 0"
            + ".015052583068609238, 0.004371461924165487, 0.0165410153567791, -0.01025077048689127, 0"
            + ".021938202902674675, 0.015285555273294449, -0.0032761695329099894, 0.004368226043879986, 0"
            + ".017188161611557007, 5.50578479305841e-05, 0.013020549900829792, -0.017058731988072395, 0"
            + ".028008418157696724, -0.006976218894124031, -0.0093706538900733, 0.0018346549477428198, 0"
            + ".004284097347408533, -0.0030496688559651375, -0.037793245166540146, 0.0076168919913470745, 0"
            + ".0067561897449195385, -0.011150301434099674, -0.004352047573775053, 0.013602979481220245, 0"
            + ".03587769716978073, 0.033289119601249695, 0.025148039683699608, -0.0011017636861652136, -0"
            + ".012153375893831253, -0.013357064686715603, -0.0019026051741093397, -0.0013541501248255372, 0"
            + ".005050963722169399, -0.0024284101091325283, -0.021705230697989464, 0.017679991200566292, -0"
            + ".01985439658164978, -0.006956804543733597, 0.013564150780439377, -0.028914421796798706, -0"
            + ".0023556062951684, -0.004724155645817518, 0.019323738291859627, 0.025536326691508293, -0"
            + ".018728364259004593, -0.020812170580029488, -0.006723832339048386, -0.037793245166540146, 0"
            + ".005358357448130846, 0.00885940995067358, 0.0005941596464253962, 0.01080731488764286, -0"
            + ".012496362440288067, -0.006503803189843893, 0.004960363265126944, -0.008529365994036198, 0"
            + ".016217444092035294, -0.005918137263506651, 0.0011236048303544521, 0.019233137369155884, -0"
            + ".001074260100722313, -0.004911827389150858, -0.004863291513174772, -0.01485844049602747, 0"
            + ".0006196409813128412, 0.029225049540400505, 0.0034557522740215063, -0.01426306739449501, -0"
            + ".0018799550598487258, -0.01153858844190836, 0.03359974920749664, 0.008238150738179684, 0"
            + ".0018928979989141226, -0.02894030697643757, -0.021407542750239372, -0.00821873638778925, -2"
            + ".3256759504874935e-06, 0.009642454795539379, 0.012664619833230972, -0.018676593899726868, -0"
            + ".009532440453767776, 0.005067142192274332, -0.0015256433980539441, -0.00651351036503911, 0"
            + ".03533409535884857, 0.010677886195480824, -0.004002589266747236, -0.027568360790610313, -0"
            + ".015285555273294449, 0.02117457240819931, 0.05881250277161598, 0.02611875720322132, -0"
            + ".0014342342037707567, 0.0028086076490581036, 0.016722217202186584, -0.021731115877628326, -0"
            + ".018417734652757645, -0.003449280746281147, 0.01679987460374832, -0.004698270000517368, 0"
            + ".011493287980556488, -0.0012878177221864462, 0.012360461987555027, 0.0037178457714617252, -0"
            + ".011221487075090408, 0.004309982992708683, -0.00970716867595911, -0.00343957357108593, -0"
            + ".009545383043587208, -0.018443621695041656, -0.0028668507002294064, 0.0025740177370607853, -0"
            + ".00690503278747201, 0.005442486144602299, 0.03533409535884857, 0.003517230972647667, 0"
            + ".02395729348063469, 0.029639223590493202, 0.02873321995139122, -0.01519495528191328, -0"
            + ".02148520015180111, -0.0011947907041758299, -0.018340077251195908, 0.0021646986715495586, 0"
            + ".010509628802537918, -0.02002265490591526, 0.027361273765563965, 0.028215505182743073, 0"
            + ".008865880779922009, -0.025083325803279877, 0.0058534229174256325, 0.0016259507974609733, 0"
            + ".017239931970834732, -0.011519174091517925, 0.010800843127071857, -0.011053229682147503, -0"
            + ".006866204086691141, 0.022650061175227165, -0.0010548457503318787, -0.0374826155602932, 0"
            + ".02290891855955124, 0.02310306206345558, -0.023983178660273552, 0.0010928654810413718, -0"
            + ".0033263233490288258, 0.023025404661893845, -0.0028959722258150578, 0.001330691040493548, -0"
            + ".012308690696954727, 0.0019187837606295943, -0.020074425265192986, -0.023633720353245735, 0"
            + ".01758939027786255, 0.011092058382928371, -0.014780783094465733, -0.023323090746998787, -0"
            + ".013473550789058208, -0.0109949866309762, -0.011331502348184586, -0.0008012459147721529, 0"
            + ".0020514484494924545, -0.0008149977657012641, -0.025432782247662544, 0.006801489740610123, 0"
            + ".03277140110731125, 0.02148520015180111, -0.014897269196808338, -0.0030205475632101297, -0"
            + ".013175863772630692, -0.0013654751237481833, -0.0173823032528162, -0.02175700105726719, 0"
            + ".0024623852223157883, -0.02151108719408512, -0.012645205482840538, -0.0009529204107820988, 4"
            + ".231719140079804e-05, -0.007882221601903439, 2.79333908110857e-05, 0.009487139992415905, 0"
            + ".009215339086949825, 0.01670927368104458, 0.005711051169782877, -0.01795179210603237, -0"
            + ".0004018363542854786, -0.006099337711930275, -0.006594403646886349, 0.0003846465842798352, 0"
            + ".0014957130188122392, 0.007526291534304619, 0.00036947912303730845, 0.0013590037124231458, -0"
            + ".013512379489839077, -0.006814432796090841, -0.002059537684544921, 0.0005241871112957597, 0"
            + ".016282157972455025, 0.022921862080693245, -0.013861837796866894, -0.013590036891400814, 0"
            + ".05353180319070816, -0.00641967449337244, 0.008011650294065475, -0.012936420738697052, -0"
            + ".0006669634021818638, 0.022481802850961685, 0.009745997376739979, 0.009745997376739979, -0"
            + ".0140300951898098, -0.011117944493889809, -0.019259022548794746, -0.02351723425090313, 0"
            + ".002206763019785285, 0.00877528078854084, 0.0007785958587191999, -0.007869278080761433, 0"
            + ".026442328467965126, -0.034816380590200424, -0.01006309874355793, -0.013447664678096771, 0"
            + ".0076168919913470745, 0.00959068350493908, -0.008406408131122589, -0.008568194694817066, -0"
            + ".01972496695816517, -0.01731758937239647, 0.006293481215834618, 0.024707980453968048, 0"
            + ".018728364259004593, -0.03145122900605202, -0.0010823493357747793, 0.010503157041966915, -0"
            + ".0037631457671523094, -0.004335868638008833, -0.0010208706371486187, -0.021459314972162247, 0"
            + ".00268888589926064, -0.01343472208827734, 0.0014164377935230732, 0.010600228793919086, 0"
            + ".008781752549111843, -0.0032389587722718716, 0.0035819453187286854, 0.02066979929804802, 0"
            + ".01016664132475853, -0.023336034268140793, -0.027024758979678154, 0.016592787578701973, 0"
            + ".009648925624787807, 0.013201749883592129, 0.03862158954143524, 0.0009537293808534741, 0"
            + ".02029445394873619, 0.005840479861944914, -0.013900666497647762, 0.010134284384548664, -0"
            + ".004309982992708683, 0.004876234568655491, -0.006982690189033747, 0.022753603756427765, 0"
            + ".05267757177352905, 0.031968943774700165, -0.005830772686749697, -0.0040220036171376705, 0"
            + ".01809416338801384, 0.00843876600265503, 0.003915224689990282, -0.015233783982694149, -0"
            + ".007869278080761433, -0.027180073782801628, 0.003604595549404621, -0.0017828834243118763, 0"
            + ".002671089256182313, 0.022857148200273514, 0.03303026035428047, -0.003607831196859479, 0"
            + ".014767839573323727, 0.015660898759961128, 0.0069632758386433125, -0.006079923361539841, 0"
            + ".023012463003396988, -0.022248830646276474, -0.005736936815083027, -0.021083971485495567, 0"
            + ".0028247861191630363, -0.025264525786042213, 0.0033878020476549864, 0.0034881094470620155, -0"
            + ".030519340187311172, 0.010315485298633575, 0.0021388130262494087, 0.005649572238326073, 0"
            + ".003154829842969775, 0.004487948026508093, 0.0006726259016431868, 0.023711377754807472, -0"
            + ".011260315775871277, -0.025044497102499008, 0.004468533676117659, -0.016100957989692688, -0"
            + ".029561566188931465, -0.0055266148410737514, -0.03561883792281151, -0.02483741007745266, 0"
            + ".002624171320348978, -0.010762014426290989, -0.008820581249892712, 0.021666401997208595, -0"
            + ".03375506401062012, -0.015842100605368614, 0.005756351165473461, -0.0005359166534617543, 0"
            + ".026610586792230606, 0.006639703642576933, 0.02762013114988804, -0.0018751014722511172, -0"
            + ".024384409189224243, -0.010516099631786346, 0.02008736878633499, 0.005921373143792152, -0"
            + ".007383919786661863, -0.005623686593025923, 0.01572561450302601, 0.015156126581132412, -0"
            + ".01989322528243065, 0.014625468291342258, 0.025432782247662544, 0.007713963743299246, -0"
            + ".0018249477725476027, 0.02538101188838482, -0.004750041291117668, 0.017434075474739075, -0"
            + ".03212425857782364, -0.04035593941807747, -0.018107106909155846, 0.02598932757973671, -0"
            + ".01241870503872633, 0.002530335448682308, 0.013732408173382282, -0.013195278123021126, -0"
            + ".03005339577794075, 0.04656852409243584, -0.006380845792591572, 0.00274874665774405, 0"
            + ".002815078943967819, -0.02131694369018078, 0.021524028852581978, -0.016113901510834694, -0"
            + ".0117262601852417, 0.014806668274104595, -0.01591975800693035, 0.04045948013663292, 0"
            + ".00212586997076869, 0.030234595760703087, 0.011583888903260231, -0.004552662372589111, -0"
            + ".029147392138838768, 0.004714448470622301, 0.0010823493357747793, 0.020346226170659065, 0"
            + ".01802944950759411, -0.03551529720425606, -0.0035107594449073076, -0.010283127427101135, 0"
            + ".022520631551742554, -0.01551852747797966, -0.005005663726478815, -0.0002287251700181514, -0"
            + ".014703125692903996, -0.013719465583562851, 0.004542955197393894, 0.017537618055939674, -0"
            + ".018042391166090965, 0.008626437745988369, 0.004400583449751139, -0.009972498752176762, -0"
            + ".01742113195359707, -0.0256528127938509, -0.014729010872542858, -0.00984306912869215, -0"
            + ".023426635190844536, -0.010108399204909801, 0.006633232347667217, 0.011460931040346622, 0"
            + ".005426307674497366, -0.02829316258430481, 0.016424529254436493, 0.022740662097930908, -0"
            + ".008924123831093311, 0.025872841477394104, -0.023556062951683998, 0.0021744058467447758, -0"
            + ".010639057494699955, -0.0002372189483139664, -0.005290407221764326, -0.008930595591664314, 0"
            + ".025497497990727425, -0.031218254938721657, -0.015971528366208076, -0.021679343655705452, 0"
            + ".0055686794221401215, 0.014120695181190968, 0.04025239497423172, -0.006827375385910273, 0"
            + ".040744222700595856, -0.0015757970977574587, 0.0196861382573843, -0.015635013580322266, -0"
            + ".011661545373499393, 0.022455917671322823, -0.013913609087467194, 0.012017475441098213, 0"
            + ".009021195583045483, 0.009163567796349525, 0.015479698777198792, 0.01880602166056633, 0"
            + ".020527426153421402, 0.01859893649816513, -0.009292996488511562, 0.002499595982953906, 0"
            + ".005264521576464176, 0.031011169776320457, -0.003924931865185499, 5.596789560513571e-05, -0"
            + ".00411583948880434, -0.004811520222574472, -0.008794695138931274, -0.011053229682147503, -0"
            + ".013926551677286625, -0.0027503645978868008, 0.007506877649575472, 0.023491349071264267, -0"
            + ".02571752667427063, 0.007701020687818527, 0.0042032040655612946, -0.002611228497698903, -0"
            + ".027568360790610313, -0.004407054744660854, -0.006775604095309973, -0.026248184964060783, -0"
            + ".011648602783679962, 0.007817506790161133, -0.0016340401489287615, -0.020579198375344276, -0"
            + ".00379873882047832, -0.0024785639252513647, -0.03608478233218193, -0.008561722934246063, -0"
            + ".0015127004589885473, 0.0055266148410737514, 0.019194308668375015, -0.008283451199531555, -0"
            + ".02354312129318714, 0.03093351237475872, -0.02364666387438774, 0.008607023395597935, 0"
            + ".0013598125660791993, 0.010101927444338799, -0.0011341209756210446, 0.00411583948880434, 0"
            + ".01181038934737444, 0.00021173762797843665, -0.0013808448566123843, -0.013861837796866894, 0"
            + ".011176187545061111, -0.006723832339048386, -0.0013994502369314432, -0.0058113583363592625, -0"
            + ".005953730549663305, -0.0037243172992020845, -0.018314192071557045, -0.00193334452342242, -0"
            + ".011862160637974739, 0.0027066823095083237, 0.013823009096086025, -0.0053551215678453445, -0"
            + ".01287817768752575, 0.01121501624584198, 0.007086233235895634, -0.017123445868492126, -0"
            + ".005581622011959553, -0.007066818885505199, 0.00024409485922660679, 0.013525322079658508, -0"
            + ".024850353598594666, -0.027801332995295525, 0.004371461924165487, -0.02008736878633499, 0"
            + ".01918136700987816, 0.012949363328516483, -0.013667694292962551, 0.006510274950414896, -0"
            + ".003643424017354846, 0.01126678753644228, 0.003403980517759919, -0.015065526589751244, -0"
            + ".03463517874479294, -0.02324543334543705, 0.00326969800516963, -0.010787900537252426, -0"
            + ".010347842238843441, -0.00946772564202547, 0.014457210898399353, -0.02527746744453907, 0"
            + ".02341369166970253, 0.0035819453187286854, -0.017900019884109497, -0.004827698692679405, 0"
            + ".00445559062063694, 0.013525322079658508, -0.013234106823801994, 0.015065526589751244, 0"
            + ".00026816054014489055, 0.00773337809368968, -0.01233457587659359, 0.003009222447872162, -0"
            + ".01496198307722807, -0.0028344932943582535, 0.006749718450009823, 0.019841453060507774, -0"
            + ".00646173907443881, -0.02886264957487583, -0.024177322164177895, -0.013117620721459389, -0"
            + ".002773014595732093, 0.024863295257091522, 0.19590361416339874, 0.009144153445959091, -0"
            + ".0008736452437005937, 0.045791953802108765, 0.012127489782869816, 0.012101603671908379, 0"
            + ".009021195583045483, 0.0149231543764472, -0.006749718450009823, 0.031528886407613754, 0"
            + ".003030254505574703, 0.008102250285446644, -0.009092382155358791, 0.00015268567949533463, -0"
            + ".004678855650126934, -0.024047894403338432, -0.021083971485495567, -0.030312253162264824, -0"
            + ".023607835173606873, -0.03567061200737953, -0.01436660997569561, 0.011305616237223148, -0"
            + ".006549103185534477, -0.013693579472601414, 0.03787090256810188, 0.006730304099619389, -0"
            + ".0023944349959492683, -0.0047112125903368, 0.03359974920749664, -0.00479534175246954, -0"
            + ".00189936941023916, -0.0004400583275128156, -0.0063193668611347675, 0.0013735644752159715, 0"
            + ".003776088822633028, -0.0008542308933101594, -0.011803917586803436, -0.023439576849341393, 0"
            + ".019440224394202232, -0.0037049029488116503, -0.021187514066696167, -0.011758617125451565, -0"
            + ".002996279625222087, 0.0013169392477720976, 0.002907297108322382, 0.003245430300012231, 0"
            + ".00034925586078315973, 0.00038788228994235396, 0.020048540085554123, 0.023931408300995827, -0"
            + ".013900666497647762, -0.014612524770200253, 0.019608480855822563, 0.0303640253841877, -0"
            + ".006917975842952728, 0.019466109573841095, 0.01822359301149845, 0.0037243172992020845, -0"
            + ".010477270931005478, -0.01274227723479271, -0.005102735012769699, 0.040951311588287354, -0"
            + ".011525645852088928, 0.02463032305240631, -0.0008493773057125509, 0.005063906311988831, -0"
            + ".007765735499560833, -0.010580814443528652, 0.009053553454577923, -0.02764601819217205, 0"
            + ".014974926598370075, -0.030415795743465424, -0.0025254818610846996, 0.012036889791488647, -0"
            + ".016916360706090927, -0.006769132800400257, 0.02209351770579815, 0.019776739180088043, 0"
            + ".0052774641662836075, 0.03743084520101547, -0.017058731988072395, -0.0010613171616569161, -0"
            + ".0007567547145299613, -0.009797769598662853, 0.004468533676117659, -0.03605889901518822, -0"
            + ".0005945641314610839, 0.0012902445159852505, 0.0014722539344802499, 0.006775604095309973, 0"
            + ".019259022548794746, -0.013913609087467194, 0.007468048948794603, -0.003462223568931222, 0"
            + ".01849539205431938, 0.03290083259344101, -0.015065526589751244, 0.028759106993675232, -0"
            + ".009124739095568657, 7.280377030838281e-05, -0.023193662986159325, -0.03378094732761383, 0"
            + ".012121018022298813, 0.0034913450945168734, -0.005329235922545195, -0.014004209078848362, 0"
            + ".007746321149170399, -0.007817506790161133, 0.008930595591664314, -0.00823167897760868, 0"
            + ".005067142192274332, -0.0015321148093789816, -0.0002521841670386493, 0.011700374074280262, 0"
            + ".00343957357108593, -0.00014985442976467311, -0.01057434268295765, -0.012483419850468636, 0"
            + ".003504288149997592, 0.005041256546974182, 0.013499436900019646, -0.0241902656853199, -0"
            + ".010684357024729252, 0.01182333193719387, -0.029587451368570328, -0.023426635190844536, -0"
            + ".016786931082606316, 0.0051739211194217205, 0.009558325633406639, -0.03577415272593498, 0"
            + ".014457210898399353, 0.005474843550473452, 0.0173823032528162, 0.005468371789902449, -0"
            + ".006186702288687229, -0.008386993780732155, 0.0007887074607424438, 0.02798253297805786, -0"
            + ".02710241638123989, 0.019530823454260826, -0.010600228793919086, -0.02582106925547123, 0"
            + ".004206439945846796, 0.00326969800516963, -0.004733862821012735, -0.008762338198721409, 0"
            + ".011525645852088928, -0.014301896095275879, -0.0353858657181263, -0.02008736878633499, -0"
            + ".015867985785007477, -0.021692287176847458, 0.007280376739799976, 0.009040609933435917, 0"
            + ".01223103329539299, -0.04297040030360222, -0.043384574353694916, -0.034169234335422516, 0"
            + ".015803271904587746, 0.02818961814045906, -0.034816380590200424, 0.003921696450561285, 0"
            + ".03846627473831177, -0.0061025735922157764, -0.01646335795521736, 0.0006079114391468465, -0"
            + ".165461927652359, 0.02029445394873619, 0.008328750729560852, -0.0025934320874512196, 0"
            + ".012988192029297352, -0.0010710243368521333, 0.02950979396700859, 0.01572561450302601, -0"
            + ".011732731945812702, 0.005966673139482737, 0.03082996979355812, -0.0022698596585541964, -0"
            + ".037016671150922775, -0.004119075369089842, 0.014133637771010399, -0.012852291576564312, -0"
            + ".0028215504717081785, 0.014677239581942558, 0.010548457503318787, 0.008374051190912724, 0"
            + ".02883676439523697, -0.00891118124127388, 0.028137847781181335, -0.02002265490591526, 0"
            + ".0020676269195973873, 0.01302702073007822, 0.022455917671322823, 0.018780136480927467, -0"
            + ".010017798282206059, -0.0010653617791831493, -0.004898884799331427, -0.011234430596232414, 0"
            + ".021083971485495567, 0.016657501459121704, -0.008755866438150406, -0.0033327946439385414, -0"
            + ".004177318420261145, 0.020100312307476997, -0.013266464695334435, 0.006807961035519838, 0"
            + ".017511732876300812, 0.012638733722269535, 0.028655562549829483, -0.006982690189033747, -0"
            + ".02653292939066887, 0.014172466471791267, 0.00811519380658865, -0.024306751787662506, -0"
            + ".01975085400044918, -0.010354313999414444, 0.012004532851278782, -0.029017964377999306, -0"
            + ".012496362440288067, 0.014444267377257347, 0.011894517578184605, 0.0030334903858602047, -0"
            + ".00038080415106378496, 0.006008737720549107, 0.014625468291342258, 0.012968777678906918, -0"
            + ".0035463524982333183, -0.005232164170593023, 0.01859893649816513, 0.004737098701298237, -0"
            + ".016644559800624847, -0.025199810042977333, -0.008555252104997635, 0.005044492427259684, -0"
            + ".013007606379687786, 0.017874134704470634, -0.02096748538315296, -0.0018475978868082166, -0"
            + ".013208221644163132, -0.026455271989107132, 0.009868955239653587, 0.03442809358239174, -0"
            + ".014871383085846901, 0.009610097855329514, 0.01426306739449501, -8.205591439036652e-05, -0"
            + ".001627568737603724, 0.012192204594612122, -0.02971688099205494, 0.004788869991898537, -0"
            + ".015039640478789806, 0.00997896958142519, -0.018961336463689804, 0.003753438824787736, 0"
            + ".0043488116934895515, -0.017641162499785423, 0.03380683436989784, -0.016890473663806915, -0"
            + ".011473873630166054, -0.002499595982953906, 0.006066980771720409, 0.009881897829473019, 0"
            + ".002724478719756007, 0.015065526589751244, -0.008509951643645763, -0.014444267377257347, 0"
            + ".007940464653074741, -0.01769293285906315, -0.02127811498939991, 0.004468533676117659, 0"
            + ".02741304598748684, 0.029742766171693802, 0.003403980517759919, 0.017679991200566292, 0"
            + ".024591494351625443, 0.011816860176622868, 0.007299791090190411, 0.01121501624584198, 0"
            + ".025536326691508293, 0.015816213563084602, -0.00438764039427042, 0.02012619748711586, 0"
            + ".004099661018699408, 0.013590036891400814, 0.005183628294616938, 0.005160978063941002, 0"
            + ".02883676439523697, 0.004264682997018099, -9.242034138878807e-05, 0.01704578846693039, -0"
            + ".007642777636647224, -0.004979777615517378, -0.10121341794729233, -0.012082190252840519, 0"
            + ".021860545501112938, 0.029690993949770927, -0.013952437788248062, 0.0320466011762619, 0"
            + ".010820257477462292, 0.0048827058635652065, -0.023970237001776695, 0.01575149968266487, -0"
            + ".00946772564202547, -0.02873321995139122, 0.0029995152726769447, -0.023025404661893845, 0"
            + ".03409157693386078, 0.015324383974075317, -0.015932699665427208, 0.006342017091810703, -0"
            + ".003801974467933178, 0.017874134704470634, 0.004853584337979555, -0.004830934572964907, -0"
            + ".009027667343616486, -0.007578063290566206, -0.0010386670473963022, -0.007565120235085487, -0"
            + ".019543766975402832, 0.0218864306807518, 0.0019495231099426746, 0.01761527545750141, 0"
            + ".014004209078848362, 0.002963922219350934, 0.0008574665989726782, -0.045791953802108765, -0"
            + ".0023669314105063677, 0.004669148474931717, -0.01941433735191822, -0.0011106618912890553, 0"
            + ".001112279831431806, -0.015091411769390106, -0.003130562137812376, 0.014301896095275879, 0"
            + ".01214690413326025, -0.03530820831656456, 0.034376323223114014, -0.04967482015490532, -0"
            + ".03541175276041031, 0.004630319774150848, 0.01236693374812603, -0.01758939027786255, -0"
            + ".007688078097999096, 0.006047566421329975, -0.025393953546881676, 0.0056042722426354885, 0"
            + ".017667047679424286, -0.0012344283750280738, 0.00757159199565649, -0.0032502838876098394, -0"
            + ".02527746744453907, -0.010652000084519386, -0.022378260269761086, -0.010645528323948383, 0"
            + ".002430028049275279, 0.0195825956761837, 0.01612684316933155, -0.00029121508123353124, -0"
            + ".012800520285964012, -0.025225697085261345, 0.01670927368104458, -0.033936262130737305, 0"
            + ".001469018287025392, -0.017770590260624886, -0.01910370960831642, 0.002533571096137166, -0"
            + ".013370007276535034, -0.015635013580322266, -0.0042711542919278145, -0.01546675618737936, 0"
            + ".012185732834041119, 0.003701667068526149, -0.008878824301064014, -0.0351787805557251, -0"
            + ".005575150717049837, -0.006584696471691132, 0.018132992088794708, 0.010742600075900555, 0"
            + ".013848894275724888, 0.00773337809368968, -0.009616568684577942, -0.0280601903796196, 0"
            + ".019440224394202232, 0.0023895814083516598, 0.005141563713550568, -0.026584699749946594, -0"
            + ".019297851249575615, 0.020385054871439934, 1.1445105883467477e-05, -0.0014908594312146306, 0"
            + ".013590036891400814, -0.009940140880644321, -0.00803106464445591, -0.012632262893021107, -0"
            + ".046464983373880386, 0.016748102381825447, -0.0008696005679666996, -0.006189938168972731, 0"
            + ".025562211871147156, -0.004060832317918539, -0.002705064369365573, -0.0069373901933431625, -0"
            + ".0016777224373072386, 0.01795179210603237, -0.022688889876008034, 0.006134930998086929, 0"
            + ".0009933669352903962, 0.00017836922779679298, -0.004303511697798967, -0.029535679146647453, 0"
            + ".025225697085261345, -0.0010030741104856133, 0.010554928332567215, 0.012153375893831253, 0"
            + ".014949040487408638, 0.014444267377257347, 0.014250123873353004, 0.0014787254622206092, -0"
            + ".010904386639595032, 0.0029558329842984676, -0.005358357448130846, 0.040433596819639206, -0"
            + ".017744705080986023, -0.01799062080681324, 0.010820257477462292, -0.022779490798711777, -0"
            + ".0018136227736249566, 0.017162274569272995, -0.01025077048689127, -0.018353020772337914, -0"
            + ".006542631890624762, 0.005924609024077654, 0.01704578846693039, -0.0001178005404653959, -0"
            + ".023840807378292084, -0.0065329247154295444, 0.03574826940894127, -0.0005876881768926978, -0"
            + ".012133961543440819, 0.002150137908756733, -0.007591006346046925, 0.007079761940985918, 0"
            + ".008548780344426632, 0.0008574665989726782, 0.037353187799453735, 0.01742113195359707, 0"
            + ".015285555273294449, -0.023387806490063667, -0.012800520285964012, -0.014288952574133873, 0"
            + ".013305293396115303, 0.0003504692576825619, 0.00816696509718895, -0.02008736878633499, 0"
            + ".0017667047213762999, 0.030881740152835846, -0.01302702073007822, -0.00582106551155448, -0"
            + ".016515130177140236, -0.004934477619826794, -0.012580491602420807, -0.020281512290239334, 0"
            + ".00553955789655447, -0.05446368828415871, -0.01190746109932661, -0.0008158066775649786, 0"
            + ".019983826205134392, -0.0036660742480307817, 0.016968131065368652, -0.0066785323433578014, 0"
            + ".0019204015843570232, -0.012457533739507198, -0.0053195287473499775, 0.032072488218545914, 0"
            + ".009157096035778522, 0.012774634175002575, -0.02548455446958542, 0.015932699665427208, 0"
            + ".026338785886764526, 0.03831095993518829, -0.02950979396700859, 0.016994018107652664, 0"
            + ".00551367225125432, 0.0033780948724597692, -0.02270183339715004, -0.005005663726478815, 0"
            + ".0210451427847147, -0.024164380505681038, 0.011693903245031834, 0.04058890789747238, -0"
            + ".01656690239906311, 0.018275363370776176, -0.003336030524224043, 0.010341370478272438, 0"
            + ".02117457240819931, 0.004086717963218689, -0.025199810042977333, -0.02852613478899002, -0"
            + ".021627573296427727, -0.02287008985877037, -0.0318395160138607, -0.02337486296892166, -0"
            + ".01381006557494402, 0.012069246731698513, 0.0162303876131773, -0.007882221601903439, 0"
            + ".011234430596232414, 0.025458669289946556, -0.030182823538780212, 0.0210451427847147, -0"
            + ".019336679950356483, -0.03838861733675003, 0.004530012141913176, 0.010211941786110401, 0"
            + ".042711544781923294, 0.0055686794221401215, 0.03178774192929268, 0.008056950755417347, 0"
            + ".0007288465858437121, 0.0068338471464812756, 0.01047080010175705, -0.01463841088116169, -0"
            + ".003911989275366068, 0.019673196598887444, -0.009377125650644302, -0.004973306320607662, 0"
            + ".003627245547249913, -0.021200457587838173, -0.0007296555559150875, -0.01060670055449009, 0"
            + ".036809585988521576, 0.04794047400355339, -0.020268568769097328, 0.03655072674155235, -0"
            + ".011059701442718506, 0.007248019799590111, -0.01260637678205967, -0.0026079928502440453, 0"
            + ".00228765606880188, -0.0036498955450952053, 0.0039864107966423035, -0.012845820747315884, -0"
            + ".022287659347057343, -0.002739039482548833, 0.003133797785267234, 0.01761527545750141, -0"
            + ".02839670516550541, -0.010412557050585747, 0.004322926048189402, -0.013149978592991829, -0"
            + ".012353990226984024, -0.011525645852088928, 0.015013755299150944, 0.0022601524833589792, 0"
            + ".013253521174192429, 0.002197055844590068, 0.013421779498457909, -0.010833200998604298, -0"
            + ".0075133489444851875, 0.020475655794143677, -0.004141725599765778, -0.006164052523672581, -0"
            + ".03717198595404625, 0.0025659282691776752, 0.011363859288394451, -0.04061479493975639, -0"
            + ".024034950882196426, 0.006477917544543743, 0.015156126581132412, -0.017058731988072395, -0"
            + ".0019204015843570232, 0.029147392138838768, 0.01043844223022461, -0.022947747260332108, 0"
            + ".02435852214694023, -0.007791621144860983, -0.005927844438701868, 0.006183466874063015, -0"
            + ".020385054871439934, -0.001414010999724269, -0.018391849473118782, -0.004025239497423172]";
    String q6 =
        "select ProductId, UserId, l2_distance(embedding, " + vectorArrayLiteral + ") as l2_dist, n_tokens, combined "
            + "from fineFoodReviews "
            + "order by l2_dist ASC "
            + "limit 10";
    printStatus(Quickstart.Color.YELLOW, "Search the most relevant review with the embedding of tomato soup");
    printStatus(Quickstart.Color.CYAN, "Query : " + q6);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q6)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q7 =
        "select ProductId, UserId, l2_distance(embedding, " + vectorArrayLiteral + ") as l2_dist, n_tokens, combined "
            + "from fineFoodReviews "
            + "where VECTOR_SIMILARITY(embedding," + vectorArrayLiteral + ", 5) "
            + "order by l2_dist ASC ";
    printStatus(Quickstart.Color.YELLOW,
        "Search the top 5 most relevant review with the embedding of tomato soup using HNSW index");
    printStatus(Quickstart.Color.CYAN, "Query : " + q7);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q7)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");
  }
}
