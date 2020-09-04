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
package org.apache.pinot.hadoop.data;

import com.google.common.base.Preconditions;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.RowBasedSchemaValidationResults;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.IngestionSchemaValidator;
import org.apache.pinot.spi.data.SchemaValidationResults;
import org.apache.pinot.spi.data.SchemaValidatorFactory;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class IngestionSchemaValidatorTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "QueryExecutorTest");

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    Assert.assertTrue(INDEX_DIR.mkdirs());
  }

  @Test
  public void testAvroIngestionSchemaValidatorSingleValueColumns()
      throws Exception {
    String inputFilePath = new File(
        Preconditions.checkNotNull(IngestionSchemaValidatorTest.class.getClassLoader().getResource("data/test_sample_data.avro"))
            .getFile()).toString();
    String recordReaderClassName = "org.apache.pinot.plugin.inputformat.avro.AvroRecordReader";

    Schema pinotSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("column1", FieldSpec.DataType.LONG)
        .addSingleValueDimension("column2", FieldSpec.DataType.INT)
        .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column7", FieldSpec.DataType.STRING)
        .addMetric("met_impressionCount", FieldSpec.DataType.LONG).build();

    IngestionSchemaValidator ingestionSchemaValidator =
        SchemaValidatorFactory.getSchemaValidator(pinotSchema, recordReaderClassName, inputFilePath);
    Assert.assertNotNull(ingestionSchemaValidator);
    Assert.assertFalse(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getDataTypeMismatchResult().isMismatchDetected());
    Assert.assertFalse(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getSingleValueMultiValueFieldMismatchResult().isMismatchDetected());
    Assert.assertFalse(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getMultiValueStructureMismatchResult().isMismatchDetected());
    Assert.assertFalse(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getMissingPinotColumnResult().isMismatchDetected());

    // Adding one extra column
    pinotSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("column1", FieldSpec.DataType.LONG)
        .addSingleValueDimension("column2", FieldSpec.DataType.INT)
        .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
        .addSingleValueDimension("extra_column", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column7", FieldSpec.DataType.STRING)
        .addMetric("met_impressionCount", FieldSpec.DataType.LONG).build();

    ingestionSchemaValidator =
        SchemaValidatorFactory.getSchemaValidator(pinotSchema, recordReaderClassName, inputFilePath);
    Assert.assertNotNull(ingestionSchemaValidator);
    Assert.assertFalse(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getDataTypeMismatchResult().isMismatchDetected());
    Assert.assertFalse(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getSingleValueMultiValueFieldMismatchResult().isMismatchDetected());
    Assert.assertFalse(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getMultiValueStructureMismatchResult().isMismatchDetected());
    Assert.assertTrue(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getMissingPinotColumnResult().isMismatchDetected());
    Assert.assertNotNull(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getMissingPinotColumnResult().getMismatchReason());

    // Change the data type of column1 from LONG to STRING
    pinotSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("column1", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column2", FieldSpec.DataType.INT)
        .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column7", FieldSpec.DataType.STRING)
        .addMetric("met_impressionCount", FieldSpec.DataType.LONG).build();
    ingestionSchemaValidator =
        SchemaValidatorFactory.getSchemaValidator(pinotSchema, recordReaderClassName, inputFilePath);
    Assert.assertNotNull(ingestionSchemaValidator);
    Assert.assertTrue(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getDataTypeMismatchResult().isMismatchDetected());
    Assert.assertNotNull(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getDataTypeMismatchResult().getMismatchReason());
    Assert.assertFalse(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getSingleValueMultiValueFieldMismatchResult().isMismatchDetected());
    Assert.assertFalse(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getMultiValueStructureMismatchResult().isMismatchDetected());
    Assert.assertFalse(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getMissingPinotColumnResult().isMismatchDetected());

    // Change column2 from single-value column to multi-value column
    pinotSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("column1", FieldSpec.DataType.LONG)
        .addMultiValueDimension("column2", FieldSpec.DataType.INT)
        .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column7", FieldSpec.DataType.STRING)
        .addMetric("met_impressionCount", FieldSpec.DataType.LONG).build();
    ingestionSchemaValidator =
        SchemaValidatorFactory.getSchemaValidator(pinotSchema, recordReaderClassName, inputFilePath);
    Assert.assertNotNull(ingestionSchemaValidator);
    Assert.assertFalse(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getDataTypeMismatchResult().isMismatchDetected());
    Assert.assertTrue(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getSingleValueMultiValueFieldMismatchResult().isMismatchDetected());
    Assert.assertNotNull(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getSingleValueMultiValueFieldMismatchResult().getMismatchReason());
    Assert.assertTrue(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getMultiValueStructureMismatchResult().isMismatchDetected());
    Assert.assertNotNull(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getMultiValueStructureMismatchResult().getMismatchReason());
    Assert.assertFalse(ingestionSchemaValidator.getFileBasedSchemaValidationResults().getMissingPinotColumnResult().isMismatchDetected());
  }

  @Test
  public void testAvroIngestionValidatorForMultiValueColumns()
      throws Exception {
    String tableName = "testTable";
    File avroFile = new File(Preconditions.checkNotNull(
        IngestionSchemaValidatorTest.class.getClassLoader().getResource("data/test_sample_data_multi_value.avro"))
        .getFile());

    // column 2 is of int type in the AVRO.
    // column3 and column16 are both of array of map structure.
    Schema pinotSchema = new Schema.SchemaBuilder().addSingleValueDimension("column1", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column2", FieldSpec.DataType.LONG)
        .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
        .addMultiValueDimension("column16", FieldSpec.DataType.STRING)
        .addMetric("metric_nus_impressions", FieldSpec.DataType.LONG).build();

    SegmentGeneratorConfig segmentGeneratorConfig =
        new SegmentGeneratorConfig(new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build(),
            pinotSchema);
    segmentGeneratorConfig.setInputFilePath(avroFile.getAbsolutePath());
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGeneratorConfig.setTableName(tableName);

    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();

    IngestionSchemaValidator ingestionSchemaValidator = driver.getIngestionSchemaValidator();
    SchemaValidationResults fileBasedSchemaValidationResults = ingestionSchemaValidator.getFileBasedSchemaValidationResults();
    RowBasedSchemaValidationResults rowBasedSchemaValidationResults = ingestionSchemaValidator.getRowBasedSchemaValidationResults();

    // File based schema validation
    Assert.assertTrue(fileBasedSchemaValidationResults.getDataTypeMismatchResult().isMismatchDetected());
    Assert.assertTrue(fileBasedSchemaValidationResults.getSingleValueMultiValueFieldMismatchResult().isMismatchDetected());
    Assert.assertTrue(fileBasedSchemaValidationResults.getMultiValueStructureMismatchResult().isMismatchDetected());
    Assert.assertFalse(fileBasedSchemaValidationResults.getMissingPinotColumnResult().isMismatchDetected());

    // Row based schema validation
    Assert.assertTrue(rowBasedSchemaValidationResults.getDataTypeMismatchResult().isMismatchDetected());
    Assert.assertNotNull(rowBasedSchemaValidationResults.getDataTypeMismatchResult().getMismatchReason());
    Assert
        .assertTrue(rowBasedSchemaValidationResults.getSingleValueMultiValueFieldMismatchResult().isMismatchDetected());
    Assert.assertNotNull(
        rowBasedSchemaValidationResults.getSingleValueMultiValueFieldMismatchResult().getMismatchReason());
    Assert.assertTrue(rowBasedSchemaValidationResults.getMultiValueStructureMismatchResult().isMismatchDetected());
    Assert.assertNotNull(rowBasedSchemaValidationResults.getMultiValueStructureMismatchResult().getMismatchReason());

    // The results between file based and row based validation matches
    Assert.assertEquals(rowBasedSchemaValidationResults.getDataTypeMismatchResult().isMismatchDetected(),
        fileBasedSchemaValidationResults.getDataTypeMismatchResult().isMismatchDetected());
    Assert.assertEquals(
        rowBasedSchemaValidationResults.getSingleValueMultiValueFieldMismatchResult().isMismatchDetected(),
        fileBasedSchemaValidationResults.getSingleValueMultiValueFieldMismatchResult().isMismatchDetected());
    Assert.assertEquals(rowBasedSchemaValidationResults.getMultiValueStructureMismatchResult().isMismatchDetected(),
        fileBasedSchemaValidationResults.getMultiValueStructureMismatchResult().isMismatchDetected());
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
