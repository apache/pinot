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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


@Test(suiteName = "CustomClusterIntegrationTest")
public class SumPrecisionTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "SumPrecisionTest";

  private static final int NUM_DOCS = 1000;
  private static final String DIM_NAME = "dimName";
  private static final String MET_BIG_DECIMAL_BYTES = "metBigDecimalBytes";
  private static final String MET_BIG_DECIMAL_STRING = "metBigDecimalString";
  private static final String MET_DOUBLE = "metDouble";
  private static final String MET_LONG = "metLong";

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(DIM_NAME, FieldSpec.DataType.STRING)
        .addMetric(MET_BIG_DECIMAL_BYTES, FieldSpec.DataType.BIG_DECIMAL)
        .addMetric(MET_BIG_DECIMAL_STRING, FieldSpec.DataType.BIG_DECIMAL)
        .addMetric(MET_DOUBLE, FieldSpec.DataType.DOUBLE)
        .addMetric(MET_LONG, FieldSpec.DataType.LONG).build();
  }

  @Override
  public File createAvroFile()
      throws IOException {

    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(DIM_NAME, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(MET_BIG_DECIMAL_BYTES, org.apache.avro.Schema.create(
            org.apache.avro.Schema.Type.BYTES), null, null),
        new org.apache.avro.Schema.Field(MET_BIG_DECIMAL_STRING, org.apache.avro.Schema.create(
            org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(MET_DOUBLE, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
            null, null),
        new org.apache.avro.Schema.Field(MET_LONG, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null)));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      int dimCardinality = 50;
      BigDecimal bigDecimalBase = BigDecimal.valueOf(Integer.MAX_VALUE + 1L);
      for (int i = 0; i < getCountStarResult(); i++) {
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

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT SUMPRECISION(%s), SUMPRECISION(%s), sum(%s), sum(%s) FROM %s",
            MET_BIG_DECIMAL_BYTES, MET_BIG_DECIMAL_STRING, MET_DOUBLE, MET_LONG, getTableName());
    double sumResult = 2147484147500L;
    JsonNode jsonNode = postQuery(query);
    for (int i = 0; i < 4; i++) {
      assertEquals(Double.parseDouble(jsonNode.get("resultTable").get("rows").get(0).get(i).asText()), sumResult);
    }
  }
}
