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
package org.apache.pinot.plugin.inputformat.parquet;

import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformerUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test demonstrating automatic JSON to MAP conversion using TransformConfig.
 * This approach applies transformations during record processing without explicit UDF calls.
 */
public class ParquetMapUDFTest {
  private File _tempDir;
  private File _parquetFile;

  @BeforeClass
  public void setUp() throws Exception {
    _tempDir = new File(FileUtils.getTempDirectory(), "ParquetWithTransformTest");
    FileUtils.forceMkdir(_tempDir);
    _parquetFile = new File(_tempDir, "test_events.parquet");

    // Generate test parquet file with JSON data
    generateTestParquetFile();
  }

  @AfterClass
  public void tearDown() throws Exception {
    if (_tempDir != null && _tempDir.exists()) {
      FileUtils.deleteDirectory(_tempDir);
    }
  }

  /**
   * Generates a test parquet file with sample event data including JSON strings.
   */
  private void generateTestParquetFile() throws IOException {
    // Define Avro schema for the parquet file
    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("Event")
        .fields()
        .name("id").type().stringType().noDefault()
        .name("event_type").type().stringType().noDefault()
        .name("event_properties").type().stringType().noDefault()
        .endRecord();

    // Create sample records with JSON strings
    List<GenericRecord> records = new ArrayList<>();

    for (int i = 1; i <= 10; i++) {
      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("id", "event_" + i);
      record.put("event_type", "user_action");

      // Create JSON string for event_properties
      String jsonProperties = String.format(
          "{\"user_id\": \"user_%d\", \"action\": \"click\", \"timestamp\": %d, \"page\": \"home\"}",
          i, System.currentTimeMillis() + i * 1000
      );
      record.put("event_properties", jsonProperties);

      records.add(record);
    }

    // Write to parquet file
    try (ParquetWriter<GenericRecord> writer = ParquetTestUtils.getParquetAvroWriter(
        new Path(_parquetFile.getAbsolutePath()), avroSchema)) {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    }
  }

  /**
   * Demonstrates how to use TransformConfig with stringToMap() function.
   * stringToMap() converts a STRING column (containing JSON) into a MAP data type.
   * The MAP values are kept as OBJECT type, allowing the schema to handle different value types.
   *
   * Example:
   *   Input (STRING): "{\"user_id\": \"user_1\", \"action\": \"click\", \"count\": 5}"
   *   Output (MAP): Map with entries {user_id -> "user_1", action -> "click", count -> 5}
   */
  @Test
  public void testStringToMapTransform()
      throws Exception {
    // 1. Define schema with STRING column (contains JSON) and MAP column (target)
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("events")
        .addSingleValueDimension("id", FieldSpec.DataType.STRING)
        .addSingleValueDimension("event_type", FieldSpec.DataType.STRING)
        .addSingleValueDimension("event_properties", FieldSpec.DataType.STRING) // Source: JSON STRING
        .build();

    // Add MAP column - this will hold the converted data from JSON STRING
    // Values are OBJECT type so schema can handle type conversions
    Map<String, FieldSpec> mapChildren = new HashMap<>();
    mapChildren.put("key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true));
    mapChildren.put("value", new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true));
    ComplexFieldSpec eventPropsMapSpec = new ComplexFieldSpec("event_properties_map",
        FieldSpec.DataType.MAP, true, mapChildren);
    schema.addField(eventPropsMapSpec);

    // 2. Create TableConfig with TransformConfig using stringToMap function
    List<TransformConfig> transformConfigs = new ArrayList<>();
    transformConfigs.add(new TransformConfig("event_properties_map", "stringToMap(event_properties)"));

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setIngestionConfig(ingestionConfig)
        .build();

    // 3. Create RecordTransformers (this is what happens during ingestion)
    List<RecordTransformer> transformers = RecordTransformerUtils.getDefaultTransformers(tableConfig, schema);

    // 4. Read records from generated Parquet file
    ParquetNativeRecordReader reader = new ParquetNativeRecordReader();
    reader.init(_parquetFile, ImmutableSet.of("id", "event_type", "event_properties"),
        new ParquetRecordReaderConfig());

    int recordsProcessed = 0;
    int successfulTransforms = 0;

    while (reader.hasNext() && recordsProcessed < 10) {
      GenericRow inputRow = reader.next();
      recordsProcessed++;

      for (RecordTransformer transformer : transformers) {
           transformer.transform(inputRow);
       }

      // Verify transformation created a MAP column
      Object eventPropsMap = inputRow.getValue("event_properties_map");
      Assert.assertTrue(eventPropsMap instanceof Map,
          "event_properties_map should be Map type after transform, but got: "
          + (eventPropsMap == null ? "null" : eventPropsMap.getClass().getName()));
      successfulTransforms++;
    }

    reader.close();
    Assert.assertTrue(recordsProcessed > 0, "Should have processed at least one record");
    Assert.assertTrue(successfulTransforms > 0, "Should have at least one successful transform");
  }
}
