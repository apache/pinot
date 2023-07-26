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
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class SumPrecisionIntegrationTest extends BaseClusterIntegrationTest {
  private static final String DIM_NAME = "dimName";
  private static final String MET_BIG_DECIMAL_BYTES = "metBigDecimalBytes";
  private static final String MET_BIG_DECIMAL_STRING = "metBigDecimalString";
  private static final String MET_DOUBLE = "metDouble";
  private static final String MET_LONG = "metLong";

  @BeforeClass
  public void setup()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // create & upload schema AND table config
    Schema schema = new Schema.SchemaBuilder().setSchemaName(DEFAULT_SCHEMA_NAME)
        .addSingleValueDimension(DIM_NAME, FieldSpec.DataType.STRING)
        .addMetric(MET_BIG_DECIMAL_BYTES, FieldSpec.DataType.BIG_DECIMAL)
        .addMetric(MET_BIG_DECIMAL_STRING, FieldSpec.DataType.BIG_DECIMAL)
        .addMetric(MET_DOUBLE, FieldSpec.DataType.DOUBLE)
        .addMetric(MET_LONG, FieldSpec.DataType.LONG).build();
    addSchema(schema);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(DEFAULT_TABLE_NAME).build();
    addTableConfig(tableConfig);

    // create & upload segments
    File avroFile = createAvroFile(getCountStarResult());
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFile, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(DEFAULT_TABLE_NAME, _tarDir);

    waitForAllDocsLoaded(60_000);
  }

  @Override
  protected long getCountStarResult() {
    return 1000;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT SUMPRECISION(%s), SUMPRECISION(%s), sum(%s), sum(%s) FROM %s",
            MET_BIG_DECIMAL_BYTES, MET_BIG_DECIMAL_STRING, MET_DOUBLE, MET_LONG, DEFAULT_TABLE_NAME);
    double sumResult = 2147484147500L;
    JsonNode jsonNode = postQuery(query);
    System.out.println("jsonNode = " + jsonNode.toPrettyString());
    for (int i = 0; i < 4; i++) {
      assertEquals(Double.parseDouble(jsonNode.get("resultTable").get("rows").get(0).get(i).asText()), sumResult);
    }
  }

  private void runAndAssert(String query, Map<String, Integer> expectedGroupToValueMap)
      throws Exception {
    Map<String, Integer> actualGroupToValueMap = new HashMap<>();
    JsonNode jsonNode = postQuery(query);
    jsonNode.get("resultTable").get("rows").forEach(node -> {
      String group = node.get(0).textValue();
      int value = node.get(1).intValue();
      actualGroupToValueMap.put(group, value);
    });
    assertEquals(actualGroupToValueMap, expectedGroupToValueMap);
  }

  private File createAvroFile(long totalNumRecords)
      throws IOException {

    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new Field(DIM_NAME, org.apache.avro.Schema.create(Type.STRING), null, null),
        new Field(MET_BIG_DECIMAL_BYTES, org.apache.avro.Schema.create(Type.BYTES), null, null),
        new Field(MET_BIG_DECIMAL_STRING, org.apache.avro.Schema.create(Type.STRING), null, null),
        new Field(MET_DOUBLE, org.apache.avro.Schema.create(Type.DOUBLE), null, null),
        new Field(MET_LONG, org.apache.avro.Schema.create(Type.LONG), null, null)));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      int dimCardinality = 50;
      BigDecimal bigDecimalBase = BigDecimal.valueOf(Integer.MAX_VALUE + 1L);
      for (int i = 0; i < totalNumRecords; i++) {
        // create avro record
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(DIM_NAME, "dim" + (RandomUtils.nextInt() % dimCardinality));
        BigDecimal bigDecimalValue = bigDecimalBase.add(BigDecimal.valueOf(i));

        record.put(MET_BIG_DECIMAL_BYTES, ByteBuffer.wrap(BigDecimalUtils.serialize(bigDecimalValue)));
        record.put(MET_BIG_DECIMAL_STRING, bigDecimalValue.toPlainString());
        record.put(MET_DOUBLE, bigDecimalValue.doubleValue());
        record.put(MET_LONG, bigDecimalValue.longValue());

        // add avro record to file
        fileWriter.append(record);
      }
    }
    return avroFile;
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    dropOfflineTable(DEFAULT_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
