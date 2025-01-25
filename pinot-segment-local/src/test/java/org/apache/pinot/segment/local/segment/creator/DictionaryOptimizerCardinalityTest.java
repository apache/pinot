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
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
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
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DictionaryOptimizerCardinalityTest implements PinotBuffersAfterClassCheckRule {

  private static final Logger LOGGER = LoggerFactory.getLogger(DictionaryOptimizerCardinalityTest.class);
  private static final File INDEX_DIR = new File(DictionaryOptimizerCardinalityTest.class.toString());
  private static File _segmentDirectory;
  private static File _csvFile;

  // Test cardinality based dictionary optimization for var-length data type columns
  @Test
  public void testDictionaryForMixedCardinalitiesStringType()
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
        if ("low_cardinality_strings".equals(columnName)) {
          Assert.assertTrue(heapSegment.getForwardIndex(columnName).isDictionaryEncoded(),
              "Low cardinality columns should be dictionary encoded");
        }

        if ("high_cardinality_strings".equals(columnName)) {
          Assert.assertFalse(heapSegment.getForwardIndex(columnName).isDictionaryEncoded(),
              "High cardinality columns should be raw encoded");
        }
      }
    } finally {
      heapSegment.destroy();
    }
  }

  @BeforeClass
  private void setup()
      throws Exception {

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
    INDEX_DIR.mkdirs();
    _csvFile = new File(INDEX_DIR, "data.csv");
    generateCsv(_csvFile, 500);

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setRowTimeValueCheck(false);
    ingestionConfig.setSegmentTimeValueCheck(false);
    Schema schema =
        new Schema.SchemaBuilder()
            .addSingleValueDimension("low_cardinality_strings", FieldSpec.DataType.STRING)
            .addSingleValueDimension("high_cardinality_strings", FieldSpec.DataType.STRING)
            .addDateTimeField("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
            .build();

    List<DimensionFieldSpec> stringColumns =
        schema.getDimensionFieldSpecs().stream().filter(x -> x.getDataType() == FieldSpec.DataType.STRING)
            .collect(Collectors.toList());

    List<FieldConfig> fieldConfigList = stringColumns.stream().map(
            x -> new FieldConfig(x.getName(), FieldConfig.EncodingType.DICTIONARY, Collections.emptyList(), null, null))
        .collect(Collectors.toList());

    SegmentGeneratorConfig segmentGenSpec =
        new SegmentGeneratorConfig(new TableConfigBuilder(TableType.OFFLINE).setTableName("tableName")
            .setIngestionConfig(ingestionConfig).setFieldConfigList(fieldConfigList).build(),
            schema);

    segmentGenSpec.setInputFilePath(_csvFile.getAbsolutePath());
    segmentGenSpec.setTimeColumnName("ts");
    segmentGenSpec.setSegmentTimeUnit(TimeUnit.SECONDS);
    segmentGenSpec.setFormat(FileFormat.CSV);
    segmentGenSpec.setSegmentVersion(SegmentVersion.v1);
    segmentGenSpec.setTableName("tableName");
    segmentGenSpec.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGenSpec.setOptimizeDictionary(true);
    segmentGenSpec.setNoDictionaryCardinalityRatioThreshold(0.1); // cardinality must be <10% of total docs to override

    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(segmentGenSpec);
    driver.build();

    _segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());
  }

  // Generate a 3 columns csv file, sample format is:
  // low_cardinality_strings,high_cardinality_strings,ts
  // Red,kdeejdfnsd,1600000000
  private void generateCsv(File file, int numberOfRows) throws IOException {
    String[] lowCardinalityOptions = {"Red", "Blue", "Green", "Yellow", "Purple"};
    String alphabet = "abcdefghijklmnopqrstuvwxyz";
    Random random = new Random(42);

    try (FileWriter writer = new FileWriter(file, false)) {
      // Write the header
      writer.append("low_cardinality_strings,high_cardinality_strings,ts\n");

      long startTimestamp = System.currentTimeMillis() / 1000;
      for (int i = 0; i < numberOfRows; i++) {
        String lowCardinality = lowCardinalityOptions[random.nextInt(lowCardinalityOptions.length)];
        StringBuilder highCardinality = new StringBuilder(10);
        for (int j = 0; j < 10; j++) {
          highCardinality.append(alphabet.charAt(random.nextInt(alphabet.length())));
        }
        long timestamp = startTimestamp + (i / 10);
        writer.append(String.format("%s,%s,%d\n", lowCardinality, highCardinality, timestamp));
      }
    }
  }

  @AfterClass
  public void cleanup() {
    FileUtils.deleteQuietly(_csvFile);
    FileUtils.deleteQuietly(_segmentDirectory);
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
