package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.server.util.SegmentTestUtils;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
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
public class ConvertAndQueryAvroDataTest extends ClusterTest {
  private final File _tmpDir = new File("/tmp/ConvertAndQueryAvroDataTest");

  @BeforeClass
  public void setUp() throws Exception {
    // Start the cluster
    startController();
    startBroker();
    startOfflineServer();

    // Create a data resource
    createResource("myresource");

    // Add table to resource
    addTableToResource("myresource", "mytable");

    // Convert the avro data to segments
    _tmpDir.mkdirs();

    System.out.println("Building 12 segments in parallel");
    ExecutorService executor = Executors.newCachedThreadPool();
    for(int i = 1; i <= 12; ++i) {
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
                        new File("pinot-integration-tests/src/test/resources",
                            "On_Time_On_Time_Performance_2014_" + segmentNumber + ".avro.gz"),
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

    // Upload the segments
    for(int i = 1; i <= 12; ++i) {
      System.out.println("Uploading segment " + i);
      File file = new File(_tmpDir, "myresource_mytable_" + i);
      FileUploadUtils.sendFile("localhost", "8998", "myresource_mytable_" + i, new FileInputStream(file), file.length());
    }

    // Wait 15 seconds for the last segment to be picked up by the server
    Thread.sleep(15000l);
  }

  @Test
  public void testCountStar()
      throws Exception {
    // Check that we have the right number of rows (5819811)
    JSONObject response = postQuery("select count(*) from 'myresource.mytable'");
    System.out.println("response = " + response);
    Assert.assertEquals("5819811", response.getJSONArray("aggregationResults").getJSONObject(0).getString("value"));

    // Check that we have the right number of rows per carrier
    Map<String, String> correctValues = new HashMap<String, String>();
    correctValues.put("AA", "537697");
    correctValues.put("AS", "160257");
    correctValues.put("B6", "249693");
    correctValues.put("DL", "800375");
    correctValues.put("EV", "686021");
    correctValues.put("F9", "85474");
    correctValues.put("FL", "79495");
    correctValues.put("HA", "74732");
    correctValues.put("MQ", "392701");
    correctValues.put("OO", "613030");
    correctValues.put("UA", "493528");
    correctValues.put("US", "414665");
    correctValues.put("VX", "57510");
    correctValues.put("WN", "1174633");

    Map<String, String> actualValues = new HashMap<String, String>();
    response = postQuery("select count(*) from 'myresource.mytable' group by Carrier");
    System.out.println(response);
    JSONArray aggregationResults = response.getJSONArray("aggregationResults").getJSONObject(0).getJSONArray("groupByResult");
    for(int i = 0; i < aggregationResults.length(); ++i) {
      actualValues.put(
          aggregationResults.getJSONObject(i).getJSONArray("group").getString(0),
          aggregationResults.getJSONObject(i).getString("value")
      );
    }
    Assert.assertEquals(actualValues, correctValues);
  }

  @Override
  protected String getHelixClusterName() {
    return "ConvertAndQueryAvroDataTest";
  }

  @AfterClass
  public void tearDown() throws Exception {
    stopBroker();
    stopController();
    stopOfflineServer();
    FileUtils.deleteDirectory(_tmpDir);
  }
}
