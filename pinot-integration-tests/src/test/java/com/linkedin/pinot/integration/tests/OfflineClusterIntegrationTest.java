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
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.server.util.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
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


/**
 * Integration test that converts avro data for 12 segments and runs queries against it.
 *
 * @author jfim
 */
public class OfflineClusterIntegrationTest extends ClusterTest {
  private final File _tmpDir = new File("/tmp/OfflineClusterIntegrationTest");

  private static final int SEGMENT_COUNT = 12;
  private static final int QUERY_COUNT = 1000;
  private Connection _connection;
  private QueryGenerator _queryGenerator;

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

    // Load data into H2
    ExecutorService executor = Executors.newCachedThreadPool();
    Class.forName("org.h2.Driver");
    _connection = DriverManager.getConnection("jdbc:h2:mem:");
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          File avroFile = new File(_tmpDir.getPath() + "/On_Time_On_Time_Performance_2014_1.avro");
          DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
          DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);

          Schema schema = dataFileReader.getSchema();
          List<Schema.Field> fields = schema.getFields();
          String[] columnNamesAndTypes = new String[fields.size()];
          int index = 0;
          for (Schema.Field field : fields) {
            List<Schema> types = field.schema().getTypes();
            if (types.size() == 1) {
              columnNamesAndTypes[index] = field.name() + " " + types.get(0).getName() + " not null";
            } else {
              columnNamesAndTypes[index] = field.name() + " " + types.get(0).getName();
            }

            columnNamesAndTypes[index] = columnNamesAndTypes[index].replace("string", "varchar(128)");
            ++index;
          }

          _connection.prepareCall("create table mytable(" + StringUtil.join(",", columnNamesAndTypes) + ")").execute();
          long start = System.currentTimeMillis();
          StringBuilder params = new StringBuilder("?");
          for (int i = 0; i < columnNamesAndTypes.length - 1; i++) {
            params.append(",?");
          }
          PreparedStatement statement = _connection
              .prepareStatement("INSERT INTO mytable VALUES (" + params.toString() + ")");

          dataFileReader.close();

          for(int currentSegmentIndex = 1; currentSegmentIndex <= SEGMENT_COUNT; ++currentSegmentIndex) {
            avroFile = new File(_tmpDir.getPath() + "/On_Time_On_Time_Performance_2014_" + currentSegmentIndex + ".avro");
            datumReader = new GenericDatumReader<GenericRecord>();
            dataFileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
              record = dataFileReader.next(record);
              for (int i = 0; i < columnNamesAndTypes.length; i++) {
                Object value = record.get(i);
                if (value instanceof Utf8) {
                  value = value.toString();
                }
                statement.setObject(i + 1, value);
              }
              statement.execute();
            }
            dataFileReader.close();
          }
          System.out.println("Insertion took " + (System.currentTimeMillis() - start));
        } catch (SQLException e) {
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });

    // Create segments from Avro data
    System.out.println("Building " + SEGMENT_COUNT + " segments in parallel");
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
                        new File(_tmpDir.getPath() + "/On_Time_On_Time_Performance_2014_" + segmentNumber + ".avro"),
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

    // Initialize query generator
    _queryGenerator = new QueryGenerator(
        new File(_tmpDir.getPath() + "/On_Time_On_Time_Performance_2014_1.avro"),
        "'myresource.mytable'", "mytable");
    for(int i = 1; i <= SEGMENT_COUNT; ++i) {
      _queryGenerator.addAvroData(
          new File(_tmpDir.getPath() + "/On_Time_On_Time_Performance_2014_" + i + ".avro")
      );
    }
    _queryGenerator.prepareToGenerateQueries();

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

  private void runQuery(String pqlQuery, List<String> sqlQueries) throws Exception {
    try {
      Statement statement = _connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

      // Run the query
      JSONObject response = postQuery(pqlQuery);
      JSONArray aggregationResultsArray = response.getJSONArray("aggregationResults");
      JSONObject firstAggregationResult = aggregationResultsArray.getJSONObject(0);
      if (firstAggregationResult.has("value")) {
        statement.execute(sqlQueries.get(0));
        ResultSet rs = statement.getResultSet();
        // Single value result for the aggregation, compare with the actual value
        String value = firstAggregationResult.getString("value");

        rs.first();
        String sqlValue = rs.getString(1);

        if (value != null && sqlValue != null) {
          // Strip decimals
          value = value.replaceAll("\\..*", "");
          sqlValue = sqlValue.replaceAll("\\..*", "");

          // Assert.assertEquals(value, sqlValue);
        } else {
          // Assert.assertEquals(value, sqlValue);
        }
      } else if (firstAggregationResult.has("groupByResult")) {
        // Load values from the query result
        for (int aggregationGroupIndex = 0; aggregationGroupIndex < aggregationResultsArray.length(); aggregationGroupIndex++) {
          JSONArray groupByResults = aggregationResultsArray.getJSONObject(aggregationGroupIndex).getJSONArray("groupByResult");
          if (groupByResults.length() != 0) {
            int groupKeyCount = groupByResults.getJSONObject(0).getJSONArray("group").length();

            Map<String, String> actualValues = new HashMap<String, String>();
            for (int resultIndex = 0; resultIndex < groupByResults.length(); ++resultIndex) {
              JSONArray group = groupByResults.getJSONObject(resultIndex).getJSONArray("group");
              String pinotGroupKey = "";
              for (int groupKeyIndex = 0; groupKeyIndex < groupKeyCount; groupKeyIndex++) {
                pinotGroupKey += group.getString(groupKeyIndex) + "\t";
              }

              actualValues.put(pinotGroupKey, Integer.toString((int) Double.parseDouble(groupByResults.getJSONObject(resultIndex).getString("value"))));
            }

            // Grouped result, build correct values and iterate through to compare both
            Map<String, String> correctValues = new HashMap<String, String>();
            statement.execute(sqlQueries.get(aggregationGroupIndex));
            ResultSet rs = statement.getResultSet();
            rs.beforeFirst();
            while (rs.next()) {
              String h2GroupKey = "";
              for (int groupKeyIndex = 0; groupKeyIndex < groupKeyCount; groupKeyIndex++) {
                h2GroupKey += rs.getString(groupKeyIndex + 1) + "\t";
              }
              correctValues.put(h2GroupKey, rs.getString(groupKeyCount + 1));
            }

            // Assert.assertEquals(actualValues, correctValues);
          } else {
            // No records in group by, check that the result set is empty
            // Assert.assertTrue(rs.isLast());
          }

        }
      }
    } catch (JSONException exception) {
      // Assert.fail("Query did not return valid JSON");
    }
    System.out.println();
  }

  @Test
  public void testMultipleQueries()
      throws Exception {
    QueryGenerator.Query[] queries = new QueryGenerator.Query[QUERY_COUNT];
    for (int i = 0; i < queries.length; i++) {
      queries[i] = _queryGenerator.generateQuery();
    }

    for (QueryGenerator.Query query : queries) {
      System.out.println(query.generatePql());

      runQuery(query.generatePql(), query.generateH2Sql());
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
