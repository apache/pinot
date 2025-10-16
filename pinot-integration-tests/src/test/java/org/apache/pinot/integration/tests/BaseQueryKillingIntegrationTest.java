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
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Base class for query killing integration tests.
public abstract class BaseQueryKillingIntegrationTest extends BaseClusterIntegrationTest {
  protected static final String STRING_DIM_SV1 = "stringDimSV1";
  protected static final String STRING_DIM_SV2 = "stringDimSV2";
  protected static final String INT_DIM_SV1 = "intDimSV1";
  protected static final String LONG_DIM_SV1 = "longDimSV1";
  protected static final String DOUBLE_DIM_SV1 = "doubleDimSV1";
  protected static final String BOOLEAN_DIM_SV1 = "booleanDimSV1";
  protected static final int NUM_SEGMENTS = 3;
  protected static final int NUM_DOCS_PER_SEGMENT = 3_000_000;

  protected static final String LARGE_SELECT_STAR_QUERY = "SELECT * FROM mytable LIMIT 9000000";

  protected static final String LARGE_DISTINCT_QUERY =
      "SELECT DISTINCT stringDimSV2 FROM mytable ORDER BY stringDimSV2 LIMIT 3000000";

  protected static final String LARGE_GROUP_BY_QUERY =
      "SELECT DISTINCT_COUNT_HLL(intDimSV1, 14), stringDimSV2 FROM mytable GROUP BY 2 ORDER BY 1 LIMIT 3000000";

  /// SafeTrim sort-aggregation case, pair-wise combine
  protected static final String LARGE_GROUP_BY_PAIRWISE_COMBINE_QUERY =
      "SET sortAggregateSingleThreadedNumSegmentsThreshold=1; SET sortAggregateLimitThreshold=3000001; "
          + "SELECT DISTINCT_COUNT_HLL(intDimSV1, 14), stringDimSV2 FROM mytable GROUP BY 2 ORDER BY 2 LIMIT 3000000";

  /// SafeTrim sort-aggregation case, sequential combine
  protected static final String LARGE_GROUP_BY_SEQUENTIAL_COMBINE_QUERY =
      "SET sortAggregateSingleThreadedNumSegmentsThreshold=10000; SET sortAggregateLimitThreshold=3000001; "
          + "SELECT DISTINCT_COUNT_HLL(intDimSV1, 14), stringDimSV2 FROM mytable GROUP BY 2 ORDER BY 2 LIMIT 3000000";

  protected static final String AGGREGATE_QUERY = "SELECT DISTINCT_COUNT_HLL(intDimSV1, 14) FROM mytable";
  protected static final String SELECT_STAR_QUERY = "SELECT * FROM mytable LIMIT 5";

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Create and upload the schema and table config
    Schema schema = new Schema.SchemaBuilder().setSchemaName(DEFAULT_TABLE_NAME)
        .addSingleValueDimension(STRING_DIM_SV1, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_DIM_SV2, FieldSpec.DataType.STRING)
        .addSingleValueDimension(INT_DIM_SV1, FieldSpec.DataType.INT)
        .addSingleValueDimension(LONG_DIM_SV1, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DOUBLE_DIM_SV1, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(BOOLEAN_DIM_SV1, FieldSpec.DataType.BOOLEAN)
        .build();
    addSchema(schema);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(DEFAULT_TABLE_NAME).build();
    addTableConfig(tableConfig);

    List<File> avroFiles = createAvroFiles();
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(DEFAULT_TABLE_NAME, _tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(Broker.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    brokerConf.setProperty(Broker.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    serverConf.setProperty(Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
    serverConf.setProperty(Server.CONFIG_OF_QUERY_EXECUTOR_NUM_GROUPS_LIMIT, Integer.MAX_VALUE);
  }

  @Override
  protected long getCountStarResult() {
    return NUM_SEGMENTS * NUM_DOCS_PER_SEGMENT;
  }

  @Test
  public void testResourceUsageStats()
      throws Exception {
    JsonNode queryResponse = postQuery(SELECT_STAR_QUERY);
    long offlineThreadMemAllocatedBytes = queryResponse.get("offlineThreadMemAllocatedBytes").asLong();
    long offlineResponseSerMemAllocatedBytes = queryResponse.get("offlineResponseSerMemAllocatedBytes").asLong();
    long offlineTotalMemAllocatedBytes = queryResponse.get("offlineTotalMemAllocatedBytes").asLong();

    assertTrue(offlineThreadMemAllocatedBytes > 0);
    assertTrue(offlineResponseSerMemAllocatedBytes > 0);
    assertEquals(offlineThreadMemAllocatedBytes + offlineResponseSerMemAllocatedBytes, offlineTotalMemAllocatedBytes);

    long offlineThreadCpuTimeNs = queryResponse.get("offlineThreadCpuTimeNs").asLong();
    long offlineSystemActivitiesCpuTimeNs = queryResponse.get("offlineSystemActivitiesCpuTimeNs").asLong();
    long offlineResponseSerializationCpuTimeNs = queryResponse.get("offlineResponseSerializationCpuTimeNs").asLong();
    long offlineTotalCpuTimeNs = queryResponse.get("offlineTotalCpuTimeNs").asLong();
    assertTrue(offlineThreadCpuTimeNs > 0);
    assertTrue(offlineSystemActivitiesCpuTimeNs > 0);
    assertTrue(offlineResponseSerializationCpuTimeNs > 0);
    assertEquals(offlineThreadCpuTimeNs + offlineSystemActivitiesCpuTimeNs + offlineResponseSerializationCpuTimeNs,
        offlineTotalCpuTimeNs);
  }

  @DataProvider
  public String[] expensiveQueries() {
    return new String[]{
        LARGE_SELECT_STAR_QUERY, LARGE_DISTINCT_QUERY, LARGE_GROUP_BY_QUERY, LARGE_GROUP_BY_PAIRWISE_COMBINE_QUERY,
        LARGE_GROUP_BY_SEQUENTIAL_COMBINE_QUERY
    };
  }

  protected List<File> createAvroFiles()
      throws IOException {
    // Create Avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(new Field(STRING_DIM_SV1, org.apache.avro.Schema.create(Type.STRING), null, null),
        new Field(STRING_DIM_SV2, org.apache.avro.Schema.create(Type.STRING), null, null),
        new Field(INT_DIM_SV1, org.apache.avro.Schema.create(Type.INT), null, null),
        new Field(LONG_DIM_SV1, org.apache.avro.Schema.create(Type.LONG), null, null),
        new Field(DOUBLE_DIM_SV1, org.apache.avro.Schema.create(Type.DOUBLE), null, null),
        new Field(BOOLEAN_DIM_SV1, org.apache.avro.Schema.create(Type.BOOLEAN), null, null)));

    // Create Avro files
    List<File> ret = new ArrayList<>();
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      File avroFile = new File(_tempDir, "data_" + segmentId + ".avro");
      try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        fileWriter.create(avroSchema, avroFile);

        int randBound = NUM_DOCS_PER_SEGMENT / 2;
        Random random = new Random(0);
        for (int docId = 0; docId < NUM_DOCS_PER_SEGMENT; docId++) {
          GenericData.Record record = new GenericData.Record(avroSchema);
          record.put(STRING_DIM_SV1, "test query killing");
          record.put(STRING_DIM_SV2, "test query killing" + docId);
          record.put(INT_DIM_SV1, random.nextInt(randBound));
          record.put(LONG_DIM_SV1, random.nextLong());
          record.put(DOUBLE_DIM_SV1, random.nextDouble());
          record.put(BOOLEAN_DIM_SV1, true);
          fileWriter.append(record);
        }
        ret.add(avroFile);
      }
    }
    return ret;
  }
}
