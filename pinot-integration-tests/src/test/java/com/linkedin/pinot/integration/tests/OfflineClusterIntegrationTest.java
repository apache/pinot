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

import com.linkedin.pinot.common.ZkTestUtils;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.server.util.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that converts avro data for 12 segments and runs queries against it.
 *
 * @author jfim
 */
public class OfflineClusterIntegrationTest extends ClusterTest {
  private final File _tmpDir = new File("/tmp/OfflineClusterIntegrationTest");

  private static final int SEGMENT_COUNT = 12;

  @BeforeClass
  public void setUp() throws Exception {
    // Start the cluster
    startZk();
    startController();
    startBroker();
    startOfflineServer();

    // Create a data resource
    createResource("myresource");

    // Add table to resource
    addTableToResource("myresource", "mytable");

    // Unpack the Avro files
    TarGzCompressionUtils.unTar(new File(TestUtils.getFileFromResourceUrl(OfflineClusterIntegrationTest.class.getClassLoader().getResource("On_Time_On_Time_Performance_2014_100k_subset.tar.gz"))), new File("/tmp/OfflineClusterIntegrationTest"));

    // Convert the Avro data to segments
    _tmpDir.mkdirs();

    System.out.println("Building " + SEGMENT_COUNT + " segments in parallel");
    ExecutorService executor = Executors.newCachedThreadPool();
    for(int i = 1; i <= SEGMENT_COUNT; ++i) {
      final int segmentNumber = i;

      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            // Build segment
            System.out.println("Starting to build segment " + segmentNumber);
            File outputDir = new File(_tmpDir, "segment-" + segmentNumber);
            final SegmentGeneratorConfig genConfig =
                SegmentTestUtils
                    .getSegmentGenSpecWithSchemAndProjectedColumns(
                        new File("/tmp/OfflineClusterIntegrationTest/On_Time_On_Time_Performance_2014_"
                            + segmentNumber + ".avro"),
                        outputDir,
                        "daysSinceEpoch", TimeUnit.DAYS, "myresource", "mytable");

            genConfig.setSegmentNamePostfix(Integer.toString(segmentNumber));

            final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
            driver.init(genConfig);
            driver.build();

            // Tar segment
            TarGzCompressionUtils.createTarGzOfDirectory(
                outputDir.getAbsolutePath() + "/myresource_mytable_" + segmentNumber,
                new File(outputDir.getParent(), "myresource_mytable_" + segmentNumber).getAbsolutePath());

            System.out.println("Completed segment " + segmentNumber);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      });
    }

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
          if(externalView.getId().contains("myresource")) {

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
    for(int i = 1; i <= SEGMENT_COUNT; ++i) {
      System.out.println("Uploading segment " + i);
      File file = new File(_tmpDir, "myresource_mytable_" + i);
      FileUploadUtils.sendFile("localhost", "8998", "myresource_mytable_" + i, new FileInputStream(file), file.length());
    }

    // Wait for all segments to be online
    latch.await();
  }

  @Test
  public void testMultipleQueries()
      throws Exception {
    // Check that we have the right number of rows (100348)
    JSONObject response = postQuery("select count(*) from 'myresource.mytable'");
    System.out.println("response = " + response);
    Assert.assertEquals("100348", response.getJSONArray("aggregationResults").getJSONObject(0).getString("value"));

    // Run queries from the properties file
    Properties properties = new Properties();
    properties.load(OfflineClusterIntegrationTest.class.getClassLoader().getResourceAsStream("OfflineClusterIntegrationTest.properties"));

    String[] queryNames = properties.getProperty("queries").split(",");

    for (String queryName : queryNames) {
      String pql = properties.getProperty(queryName + ".pql");
      String result = properties.getProperty(queryName + ".result");

      System.out.println(pql);

      // Load correct values
      Map<String, String> correctValues = new HashMap<String, String>();
      String[] resultTuples = result.split(", ");
      for (String resultTuple : resultTuples) {
        int commaIndex = resultTuple.indexOf(',');
        String key = resultTuple.substring(1, commaIndex);
        String value = resultTuple.substring(commaIndex + 1, resultTuple.length() - 1);
        correctValues.put(key, value);
      }

      // Run the query
      Map<String, String> actualValues = new HashMap<String, String>();
      response = postQuery(pql);
      JSONArray aggregationResults = response.getJSONArray("aggregationResults").getJSONObject(0).getJSONArray("groupByResult");
      for(int i = 0; i < aggregationResults.length(); ++i) {
        actualValues.put(
            aggregationResults.getJSONObject(i).getJSONArray("group").getString(0),
            Integer.toString((int) Double.parseDouble(aggregationResults.getJSONObject(i).getString("value")))
        );
      }
      System.out.println();

      Assert.assertEquals(actualValues, correctValues);
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
}
