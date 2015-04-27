/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.ZkTestUtils;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.util.TestUtils;


/**
 * Integration test that converts avro data for 12 segments and runs queries against it.
 *
 * @author jfim
 */
public class OfflineClusterIntegrationTest extends BaseClusterIntegrationTest {
  private final File _tmpDir = new File("/tmp/OfflineClusterIntegrationTest");
  private File queriesFile;

  private static final int SEGMENT_COUNT = 12;
  private static final int QUERY_COUNT = 1000;

  @BeforeClass
  public void setUp() throws Exception {
    // Start the cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Create a data resource
    createOfflineResource("myresource", "DaysSinceEpoch", "daysSinceEpoch");

    // Add table to resource
    addTableToOfflineResource("myresource", "mytable", "DaysSinceEpoch", "daysSinceEpoch");

    // Unpack the Avro files
    TarGzCompressionUtils.unTar(
        new File(TestUtils.getFileFromResourceUrl(OfflineClusterIntegrationTest.class.getClassLoader().getResource(
            "On_Time_On_Time_Performance_2014_100k_subset.tar.gz"))), _tmpDir);

    _tmpDir.mkdirs();

    queriesFile =
        new File(TestUtils.getFileFromResourceUrl(OfflineClusterIntegrationTest.class.getClassLoader().getResource(
            "On_Time_On_Time_Performance_2014_100k_subset.test_queries_10K")));
    final List<File> avroFiles = new ArrayList<File>(SEGMENT_COUNT);
    for (int segmentNumber = 1; segmentNumber <= SEGMENT_COUNT; ++segmentNumber) {
      avroFiles.add(new File(_tmpDir.getPath() + "/On_Time_On_Time_Performance_2014_" + segmentNumber + ".avro"));
    }

    // Load data into H2
    ExecutorService executor = Executors.newCachedThreadPool();
    Class.forName("org.h2.Driver");
    _connection = DriverManager.getConnection("jdbc:h2:mem:");
    executor.execute(new Runnable() {
      @Override
      public void run() {
        createH2SchemaAndInsertAvroFiles(avroFiles, _connection);
      }
    });

    // Create segments from Avro data
    buildSegmentsFromAvro(avroFiles, executor, 0, _tmpDir);

    // Initialize query generator
    executor.execute(new Runnable() {
      @Override
      public void run() {
        _queryGenerator = new QueryGenerator(avroFiles, "'myresource.mytable'", "mytable");
      }
    });

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Set up a Helix spectator to count the number of segments that are uploaded and unlock the latch once 12 segments are online
    final CountDownLatch latch = new CountDownLatch(1);
    HelixManager manager =
        HelixManagerFactory.getZKHelixManager(getHelixClusterName(), "test_instance", InstanceType.SPECTATOR,
            ZkTestUtils.DEFAULT_ZK_STR);
    manager.connect();
    manager.addExternalViewChangeListener(new ExternalViewChangeListener() {
      @Override
      public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
        for (ExternalView externalView : externalViewList) {
          if (externalView.getId().contains("myresource")) {

            Set<String> partitionSet = externalView.getPartitionSet();
            if (partitionSet.size() == SEGMENT_COUNT) {
              int onlinePartitionCount = 0;

              for (String partitionId : partitionSet) {
                Map<String, String> partitionStateMap = externalView.getStateMap(partitionId);
                if (partitionStateMap.containsValue("ONLINE")) {
                  onlinePartitionCount++;
                }
              }

              if (onlinePartitionCount == SEGMENT_COUNT) {
                System.out.println("Got " + SEGMENT_COUNT + " online resources, unlatching the main thread");
                latch.countDown();
              }
            }
          }
        }
      }
    });

    // Upload the segments
    for (int i = 1; i <= SEGMENT_COUNT; ++i) {
      System.out.println("Uploading segment " + i);
      File file = new File(_tmpDir, "myresource_mytable_" + i);
      FileUploadUtils
          .sendFile("localhost", "8998", "myresource_mytable_" + i, new FileInputStream(file), file.length());
    }

    // Wait for all segments to be online
    latch.await();

    while (getCurrentServingNumDocs() < 115545) {
      Thread.sleep(1000);
    }

  }

  @Override
  protected void runQuery(String pqlQuery, List<String> sqlQueries) throws Exception {
    JSONObject ret = postQuery(pqlQuery);
    ret.put("pql", pqlQuery);
    System.out.println(ret.toString(1));
    Assert.assertEquals(ret.getJSONArray("exceptions").length(), 0);
  }

  @Override
  @Test
  public void testMultipleQueries() throws Exception {
    Scanner scanner = new Scanner(queriesFile);
    scanner.useDelimiter("\n");
    String[] pqls = new String[1000];

    for (int i = 0; i < pqls.length; i++) {
      JSONObject test_case = new JSONObject(scanner.next());
      pqls[i] = test_case.getString("pql");
    }

    for (String query : pqls) {
      try {
        runQuery(query, null);
      } catch (Exception e) {
        System.out.println("pql is : " + query);
        throw new RuntimeException(e.getMessage());
      }

    }
  }

  @Override
  @Test
  public void testHardcodedQuerySet() throws Exception {
    for (String query : getHardCodedQuerySet()) {
      try {
        System.out.println(query);
        runQuery(query, Collections.singletonList(query.replace("'myresource.mytable'", "mytable")));
      } catch (Exception e) {
        // TODO: handle exception
      }
    }
  }

  @Override
  protected String getHelixClusterName() {
    return "OfflineClusterIntegrationTest";
  }

  @AfterClass
  public void tearDown() throws Exception {
    stopBroker();
    stopController();
    stopOfflineServer();
    stopZk();
    FileUtils.deleteDirectory(_tmpDir);
  }

  @Override
  protected int getGeneratedQueryCount() {
    return QUERY_COUNT;
  }

  public static void main(String[] args) throws FileNotFoundException, IOException, ArchiveException, JSONException {

    File _tmpDir = new File("/tmp/OfflineClusterIntegrationTest");

    // Unpack the Avro files
    TarGzCompressionUtils.unTar(
        new File(TestUtils.getFileFromResourceUrl(OfflineClusterIntegrationTest.class.getClassLoader().getResource(
            "On_Time_On_Time_Performance_2014_100k_subset.tar.gz"))), _tmpDir);

    _tmpDir.mkdirs();

    File f =
        new File(
            "/home/dpatel/linkedin/pinot2_0/pinot-integration-tests/src/test/resources/On_Time_On_Time_Performance_2014_100k_subset.test_queries_10K");
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f)));

    final List<File> avroFiles = new ArrayList<File>(SEGMENT_COUNT);
    for (int segmentNumber = 1; segmentNumber <= SEGMENT_COUNT; ++segmentNumber) {
      avroFiles.add(new File(_tmpDir.getPath() + "/On_Time_On_Time_Performance_2014_" + segmentNumber + ".avro"));
    }

    QueryGenerator queryGenerator = new QueryGenerator(avroFiles, "'myresource.mytable'", "mytable");
    for (int i = 0; i < 10000; i++) {
      QueryGenerator.Query q = queryGenerator.generateQuery();
      JSONObject json = new JSONObject();
      JSONArray sqls = new JSONArray(q.generateH2Sql().toArray());
      json.put("pql", q.generatePql());
      json.put("hsqls", sqls);
      bw.write(json.toString());
      bw.newLine();
      System.out.println(json.toString());
    }
    bw.close();
  }
}
