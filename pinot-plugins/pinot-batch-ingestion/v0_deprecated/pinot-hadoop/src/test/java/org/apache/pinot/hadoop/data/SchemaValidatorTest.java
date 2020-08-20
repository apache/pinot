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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.SchemaValidator;
import org.apache.pinot.spi.data.SchemaValidatorFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SchemaValidatorTest {
  @Test
  public void testAvroSchemaValidator()
      throws Exception {
    String inputFilePath = new File(
        Preconditions.checkNotNull(SchemaValidatorTest.class.getClassLoader().getResource("data/test_sample_data.avro"))
            .getFile()).toString();
    String recordReaderClassName = "org.apache.pinot.plugin.inputformat.avro.AvroRecordReader";

    Schema pinotSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("column1", FieldSpec.DataType.LONG)
        .addSingleValueDimension("column2", FieldSpec.DataType.INT)
        .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column7", FieldSpec.DataType.STRING)
        .addMetric("met_impressionCount", FieldSpec.DataType.LONG).build();

    SchemaValidator schemaValidator =
        SchemaValidatorFactory.getSchemaValidator(pinotSchema, recordReaderClassName, inputFilePath);
    Assert.assertNotNull(schemaValidator);
    Assert.assertFalse(schemaValidator.isDataTypeMismatch());
    Assert.assertFalse(schemaValidator.isSingleValueMultiValueFieldMismatch());
    Assert.assertFalse(schemaValidator.isMultiValueStructureMismatch());
    Assert.assertFalse(schemaValidator.isMissingPinotColumn());

    // Adding one extra column
    pinotSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("column1", FieldSpec.DataType.LONG)
        .addSingleValueDimension("column2", FieldSpec.DataType.INT)
        .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
        .addSingleValueDimension("extra_column", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column7", FieldSpec.DataType.STRING)
        .addMetric("met_impressionCount", FieldSpec.DataType.LONG).build();

    schemaValidator =
        SchemaValidatorFactory.getSchemaValidator(pinotSchema, recordReaderClassName, inputFilePath);
    Assert.assertNotNull(schemaValidator);
    Assert.assertFalse(schemaValidator.isDataTypeMismatch());
    Assert.assertFalse(schemaValidator.isSingleValueMultiValueFieldMismatch());
    Assert.assertFalse(schemaValidator.isMultiValueStructureMismatch());
    Assert.assertTrue(schemaValidator.isMissingPinotColumn());

    // Change the data type of column1 from LONG to STRING
    pinotSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("column1", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column2", FieldSpec.DataType.INT)
        .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column7", FieldSpec.DataType.STRING)
        .addMetric("met_impressionCount", FieldSpec.DataType.LONG).build();
    schemaValidator =
        SchemaValidatorFactory.getSchemaValidator(pinotSchema, recordReaderClassName, inputFilePath);
    Assert.assertNotNull(schemaValidator);
    Assert.assertTrue(schemaValidator.isDataTypeMismatch());
    Assert.assertFalse(schemaValidator.isSingleValueMultiValueFieldMismatch());
    Assert.assertFalse(schemaValidator.isMultiValueStructureMismatch());
    Assert.assertFalse(schemaValidator.isMissingPinotColumn());

    // Change column2 from single-value column to multi-value column
    pinotSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("column1", FieldSpec.DataType.LONG)
        .addMultiValueDimension("column2", FieldSpec.DataType.INT)
        .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column7", FieldSpec.DataType.STRING)
        .addMetric("met_impressionCount", FieldSpec.DataType.LONG).build();
    schemaValidator =
        SchemaValidatorFactory.getSchemaValidator(pinotSchema, recordReaderClassName, inputFilePath);
    Assert.assertNotNull(schemaValidator);
    Assert.assertFalse(schemaValidator.isDataTypeMismatch());
    Assert.assertTrue(schemaValidator.isSingleValueMultiValueFieldMismatch());
    Assert.assertTrue(schemaValidator.isMultiValueStructureMismatch());
    Assert.assertFalse(schemaValidator.isMissingPinotColumn());

  }
}
