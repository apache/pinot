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

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.data.Schema;


/**
 * Test that changes the schema at runtime during an integration test.
 */
public class RealtimeSchemaChangeIntegrationTest extends RealtimeClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeSchemaChangeIntegrationTest.class);

  private File getPartialSchemaFile() {
    return new File(OfflineClusterIntegrationTest.class.getClassLoader()
        .getResource("On_Time_On_Time_Performance_2014_100k_subset_nonulls_missing_columns.schema").getFile());
  }

  @Override
  protected void setUpTable(String tableName, String timeColumnName, String timeColumnType, String kafkaZkUrl,
      String kafkaTopic, File schemaFile, File avroFile) throws Exception {
    Schema schema = Schema.fromFile(schemaFile);
    addSchema(getPartialSchemaFile(), schema.getSchemaName());

    // Create the table with a different schema than the final one
    addRealtimeTable(tableName, timeColumnName, timeColumnType, 900, "Days", kafkaZkUrl, kafkaTopic,
        schema.getSchemaName(), null, null, avroFile, ROW_COUNT_FOR_REALTIME_SEGMENT_FLUSH, "Carrier",
        new ArrayList<String>(), null, null);

    // Sleep for a little bit to get some events in the realtime table
    Uninterruptibles.sleepUninterruptibly(15L, TimeUnit.SECONDS);

    // Change the schema to the final one
    updateSchema(schemaFile, schema.getSchemaName());
  }

  @Test(enabled = false)
  public void testForColumnPartialPresence() throws Exception {
    // Count the number of rows
    JSONObject response = postQuery("select count(*) from 'mytable'");
    JSONArray aggregationResultsArray = response.getJSONArray("aggregationResults");
    JSONObject firstAggregationResult = aggregationResultsArray.getJSONObject(0);
    String pinotValue = firstAggregationResult.getString("value");
    int totalRecordCount = Integer.parseInt(pinotValue);

    // Count the number of rows where the new column exists
    response = postQuery("select count(*) from 'mytable' where ArrDel15 >= -10000");
    aggregationResultsArray = response.getJSONArray("aggregationResults");
    firstAggregationResult = aggregationResultsArray.getJSONObject(0);
    pinotValue = firstAggregationResult.getString("value");
    int rowsWithNewColumn = Integer.parseInt(pinotValue);

    // Both numbers shouldn't match, since the schema was changed
    LOGGER.info("totalRecordCount = " + totalRecordCount);
    LOGGER.info("rowsWithNewColumn = " + rowsWithNewColumn);
    Assert.assertTrue(totalRecordCount != rowsWithNewColumn,
        "The number of rows consumed with the new schema is equal to the total number of rows");
  }

  @Override
  @Test(enabled = false)
  public void testHardcodedQueries()
      throws Exception {
    // Ignored.
  }

  @Override
  @Test(enabled = false)
  public void testHardcodedQuerySet()
      throws Exception {
    // Ignored.
  }

  @Override
  @Test(enabled = false)
  public void testGeneratedQueriesWithoutMultiValues()
      throws Exception {
    // Ignored.
  }

  @Override
  @Test(enabled = false)
  public void testGeneratedQueriesWithMultiValues()
      throws Exception {
    // Ignored.
  }
}
