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
package org.apache.pinot.segment.local.segment.creator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DictionaryOptimiserTest implements PinotBuffersAfterMethodCheckRule {
  private static final Logger LOGGER = LoggerFactory.getLogger(DictionaryOptimiserTest.class);

  private static final String AVRO_DATA = "data/mixed_cardinality_data.avro";
  private static final File INDEX_DIR = new File(DictionariesTest.class.toString());

  private static File _segmentDirectory;

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @BeforeClass
  public static void before()
      throws Exception {
    final String filePath = TestUtils.getFileFromResourceUrl(
        Objects.requireNonNull(DictionaryOptimiserTest.class.getClassLoader().getResource(AVRO_DATA)));
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final SegmentGeneratorConfig config =
        getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "time_column", TimeUnit.DAYS,
            "test");
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    _segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());

    final DataFileStream<GenericRecord> avroReader = AvroUtils.getAvroReader(new File(filePath));
    final org.apache.avro.Schema avroSchema = avroReader.getSchema();
    final String[] columns = new String[avroSchema.getFields().size()];
    int i = 0;
    for (final org.apache.avro.Schema.Field f : avroSchema.getFields()) {
      columns[i] = f.name();
      i++;
    }
  }

  @Test
  public void testDictionaryForMixedCardinalities()
      throws Exception {
    ImmutableSegment heapSegment = ImmutableSegmentLoader.load(_segmentDirectory, ReadMode.heap);
    try {
      Schema schema = heapSegment.getSegmentMetadata().getSchema();
      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        // Skip virtual columns
        if (fieldSpec.isVirtualColumn()) {
          continue;
        }

        String columnName = fieldSpec.getName();
        if (columnName.contains("low_cardinality")) {
          Assert.assertTrue(heapSegment.getForwardIndex(columnName).isDictionaryEncoded(),
              "No dictionary found for low cardinality columns");
        }

        if (columnName.contains("high_cardinality")) {
          Assert.assertFalse(heapSegment.getForwardIndex(columnName).isDictionaryEncoded(),
              "No Raw index for high cardinality columns");
        }

        if (columnName.contains("key")) {
          Assert.assertFalse(heapSegment.getSegmentMetadata().getColumnMetadataFor(columnName).hasDictionary(),
              "Dictionary found for text index column");
        }
      }
    } finally {
      heapSegment.destroy();
    }
  }

  public static SegmentGeneratorConfig getSegmentGenSpecWithSchemAndProjectedColumns(File inputAvro, File outputDir,
      String timeColumn, TimeUnit timeUnit, String tableName)
      throws IOException {
    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setRowTimeValueCheck(false);
    ingestionConfig.setSegmentTimeValueCheck(false);
    Schema schema = extractSchemaFromAvroWithoutTime(inputAvro);
    List<DimensionFieldSpec> stringColumns =
        schema.getDimensionFieldSpecs().stream().filter(x -> x.getDataType() == FieldSpec.DataType.STRING).collect(
        Collectors.toList());

    List<FieldConfig> fieldConfigList = stringColumns.stream()
        .map(x -> new FieldConfig(x.getName(), FieldConfig.EncodingType.DICTIONARY,
        Collections.singletonList(FieldConfig.IndexType.TEXT), null, null)).collect(Collectors.toList());

    final SegmentGeneratorConfig segmentGenSpec =
        new SegmentGeneratorConfig(new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName)
            .setIngestionConfig(ingestionConfig).setFieldConfigList(fieldConfigList).build(),
            schema);
    segmentGenSpec.setInputFilePath(inputAvro.getAbsolutePath());
    segmentGenSpec.setTimeColumnName(timeColumn);
    segmentGenSpec.setSegmentTimeUnit(timeUnit);
    segmentGenSpec.setFormat(FileFormat.AVRO);
    segmentGenSpec.setSegmentVersion(SegmentVersion.v1);
    segmentGenSpec.setTableName(tableName);
    segmentGenSpec.setOutDir(outputDir.getAbsolutePath());
    segmentGenSpec.setOptimizeDictionary(true);
    segmentGenSpec.setNoDictionarySizeRatioThreshold(0.9);
    return segmentGenSpec;
  }

  public static Schema extractSchemaFromAvroWithoutTime(File avroFile)
      throws IOException {
    DataFileStream<GenericRecord> dataStream =
        new DataFileStream<GenericRecord>(new FileInputStream(avroFile), new GenericDatumReader<GenericRecord>());
    Schema schema = new Schema();

    for (final org.apache.avro.Schema.Field field : dataStream.getSchema().getFields()) {
      try {
        SegmentTestUtils.getColumnType(field);
      } catch (Exception e) {
        LOGGER.warn("Caught exception while converting Avro field {} of type {}, field will not be in schema.",
            field.name(), field.schema().getType());
        continue;
      }
      final String columnName = field.name();
      final String pinotType = field.getProp("pinotType");

      final FieldSpec fieldSpec;
      if ((pinotType != null && "METRIC".equals(pinotType)) || columnName.contains("cardinality")) {
        if (field.schema().isUnion() && isDoubleOrFloat(field)) {
          fieldSpec = new MetricFieldSpec();
        } else {
          fieldSpec = new DimensionFieldSpec();
        }
      } else {
        fieldSpec = new DimensionFieldSpec();
      }

      fieldSpec.setName(columnName);
      fieldSpec.setDataType(SegmentTestUtils.getColumnType(dataStream.getSchema().getField(columnName)));
      fieldSpec.setSingleValueField(AvroUtils.isSingleValueField(dataStream.getSchema().getField(columnName)));
      schema.addField(fieldSpec);
    }

    dataStream.close();
    return schema;
  }

  private static boolean isDoubleOrFloat(org.apache.avro.Schema.Field field) {
    return field.schema().getTypes().contains(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE))
        || field.schema().getTypes().contains(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT));
  }
}
