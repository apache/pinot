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
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// End-to-end coverage for BYTES as a multi-valued dimension. SV BYTES is covered by [BytesTypeTest]; this test
/// exercises the MV path: ingestion (Avro `array<bytes>`), raw-MV segment storage, projection via
/// [org.apache.pinot.core.common.RowBasedBlockValueFetcher], DataTable ser/de, and broker-side extraction.
///
/// The MV BYTES column appears in two variants — dictionary-encoded (default) and raw (no-dictionary) — so the
/// tests exercise both encodings. The raw variant is configured via [#getNoDictionaryColumns()].
@Test(suiteName = "CustomClusterIntegrationTest")
public class BytesMvTypeTest extends CustomDataQueryClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "BytesMvTypeTest";
  private static final int NUM_DOCS = 100;
  private static final int MV_LENGTH = 3;

  private static final String ID_COLUMN = "id";
  // Dictionary-encoded variant (default)
  private static final String BYTES_MV_COLUMN = "bytesMV";
  // Raw (no-dictionary) variant
  private static final String RAW_BYTES_MV_COLUMN = "rawBytesMV";

  private static final String[] MV_COLUMNS = {BYTES_MV_COLUMN, RAW_BYTES_MV_COLUMN};

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setNoDictionaryColumns(getNoDictionaryColumns()).build();
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return List.of(RAW_BYTES_MV_COLUMN);
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(getTableName())
        .addSingleValueDimension(ID_COLUMN, DataType.INT)
        .addMultiValueDimension(BYTES_MV_COLUMN, DataType.BYTES)
        .addMultiValueDimension(RAW_BYTES_MV_COLUMN, DataType.BYTES)
        .build();
  }

  /// For doc `i`, MV elements are `[0xNN, 0xMM, 0xPP]` where each element encodes `i` in a different
  /// way. Chosen so that both content and order are deterministic and easy to assert.
  private static byte[][] mvValues(int i) {
    return new byte[][]{
        new byte[]{(byte) (i & 0xFF)},
        new byte[]{(byte) (i & 0xFF), (byte) ((i + 1) & 0xFF)},
        new byte[]{(byte) (i & 0xFF), (byte) ((i + 2) & 0xFF), (byte) ((i + 3) & 0xFF)}
    };
  }

  @Override
  public List<File> createAvroFiles()
      throws IOException {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    org.apache.avro.Schema bytesSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(ID_COLUMN, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            null,
            null),
        new org.apache.avro.Schema.Field(BYTES_MV_COLUMN, org.apache.avro.Schema.createArray(bytesSchema), null, null),
        new org.apache.avro.Schema.Field(RAW_BYTES_MV_COLUMN, org.apache.avro.Schema.createArray(bytesSchema), null,
            null)
    ));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < NUM_DOCS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(ID_COLUMN, i);
        // Each call wraps fresh byte[]s so the two columns don't share ByteBuffer state across writers.
        record.put(BYTES_MV_COLUMN, wrapBuffers(mvValues(i)));
        record.put(RAW_BYTES_MV_COLUMN, wrapBuffers(mvValues(i)));
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  private static List<ByteBuffer> wrapBuffers(byte[][] elements) {
    List<ByteBuffer> buffers = new ArrayList<>(elements.length);
    for (byte[] element : elements) {
      buffers.add(ByteBuffer.wrap(element));
    }
    return buffers;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testMvProjection(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    byte[][] expected = mvValues(7);
    for (String mvCol : MV_COLUMNS) {
      String query = String.format("SELECT %s FROM %s WHERE %s = 7 LIMIT 1", mvCol, getTableName(), ID_COLUMN);
      JsonNode rows = postQuery(query).get("resultTable").get("rows");
      assertEquals(rows.size(), 1);
      JsonNode mv = rows.get(0).get(0);
      assertEquals(mv.size(), MV_LENGTH);
      for (int i = 0; i < MV_LENGTH; i++) {
        // Pinot returns MV BYTES elements as hex-encoded strings in the JSON response.
        assertEquals(mv.get(i).asText(), Hex.encodeHexString(expected[i]));
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testMvCardinality(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    for (String mvCol : MV_COLUMNS) {
      String query = String.format("SELECT cardinality(%s) FROM %s WHERE %s = 0 LIMIT 1", mvCol, getTableName(),
          ID_COLUMN);
      JsonNode rows = postQuery(query).get("resultTable").get("rows");
      assertEquals(rows.get(0).get(0).asInt(), MV_LENGTH);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSelectWithMvColumn(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // SELECT id, mv returns the MV BYTES column alongside id — exercises the full projection + selection + broker
    // extract path for MV BYTES.
    for (String mvCol : MV_COLUMNS) {
      String query = String.format("SELECT %s, %s FROM %s WHERE %s = 0 LIMIT 1", ID_COLUMN, mvCol,
          getTableName(), ID_COLUMN);
      JsonNode result = postQuery(query).get("resultTable");
      JsonNode rows = result.get("rows");
      assertEquals(rows.size(), 1);
      assertEquals(rows.get(0).get(0).asInt(), 0);
      assertEquals(rows.get(0).get(1).size(), MV_LENGTH);
      assertTrue(result.get("dataSchema").get("columnDataTypes").toString().contains("BYTES_ARRAY"));
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCountStar(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT count(*) FROM %s", getTableName());
    JsonNode rows = postQuery(query).get("resultTable").get("rows");
    assertEquals(rows.get(0).get(0).asLong(), NUM_DOCS);
  }
}
