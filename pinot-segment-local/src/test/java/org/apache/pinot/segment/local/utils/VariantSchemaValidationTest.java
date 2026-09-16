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
package org.apache.pinot.segment.local.utils;

import java.nio.ByteBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.MaxLengthExceedStrategy;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.PinotDataType;
import org.apache.pinot.spi.utils.VariantEnvelope;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class VariantSchemaValidationTest {
  private static final String COLUMN = "payload";

  @Test
  public void testValidSingleValueVariantDimension() {
    Schema schema = schemaWith(new DimensionFieldSpec(COLUMN, DataType.VARIANT, true));
    SchemaUtils.validate(schema);

    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN);
    assertEquals((byte[]) fieldSpec.getDefaultNullValue(), new byte[0]);
    assertEquals(PinotDataType.getPinotDataTypeForIngestion(fieldSpec), PinotDataType.VARIANT);
    assertNull(SanitizationTransformerUtils.getSanitizedColumnInfo(fieldSpec));
  }

  @Test
  public void testRejectsMultiValueVariant() {
    Schema schema = schemaWith(new DimensionFieldSpec(COLUMN, DataType.VARIANT, false));
    IllegalStateException exception =
        expectThrows(IllegalStateException.class, () -> SchemaUtils.validate(schema));
    assertTrue(exception.getMessage().contains("VARIANT columns cannot be of multi-value type"));
    assertThrows(IllegalStateException.class,
        () -> PinotDataType.getPinotDataTypeForIngestion(schema.getFieldSpecFor(COLUMN)));
  }

  @Test
  public void testRejectsCustomDefaultNullValue() {
    byte[] encodedVariantNull =
        VariantEnvelope.encode(ByteBuffer.wrap(new byte[]{1}), ByteBuffer.wrap(new byte[]{0}));
    Schema schema =
        schemaWith(new DimensionFieldSpec(COLUMN, DataType.VARIANT, true, encodedVariantNull));

    IllegalStateException exception =
        expectThrows(IllegalStateException.class, () -> SchemaUtils.validate(schema));
    assertTrue(exception.getMessage().contains("custom default null value"));
  }

  @Test
  public void testRejectsEnvelopeCorruptingMaxLengthStrategies() {
    for (MaxLengthExceedStrategy strategy
        : new MaxLengthExceedStrategy[]{MaxLengthExceedStrategy.TRIM_LENGTH,
            MaxLengthExceedStrategy.SUBSTITUTE_DEFAULT_VALUE}) {
      DimensionFieldSpec fieldSpec = new DimensionFieldSpec(COLUMN, DataType.VARIANT, true);
      fieldSpec.setMaxLength(20);
      fieldSpec.setMaxLengthExceedStrategy(strategy);
      Schema schema = schemaWith(fieldSpec);

      IllegalStateException exception =
          expectThrows(IllegalStateException.class, () -> SchemaUtils.validate(schema));
      assertTrue(exception.getMessage().contains("max length strategy"));
      assertThrows(IllegalStateException.class,
          () -> SanitizationTransformerUtils.getSanitizedColumnInfo(fieldSpec));
    }
  }

  @Test
  public void testErrorMaxLengthStrategyNeverMutatesEnvelope() {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec(COLUMN, DataType.VARIANT, true);
    fieldSpec.setMaxLength(32);
    fieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.ERROR);
    SchemaUtils.validate(schemaWith(fieldSpec));

    SanitizationTransformerUtils.SanitizedColumnInfo columnInfo =
        SanitizationTransformerUtils.getSanitizedColumnInfo(fieldSpec);
    assertNotNull(columnInfo);
    byte[] envelope =
        VariantEnvelope.encode(ByteBuffer.wrap(new byte[]{1}), ByteBuffer.wrap(new byte[]{0}));
    SanitizationTransformerUtils.SanitizationResult result =
        SanitizationTransformerUtils.sanitizeValue(columnInfo, envelope);
    assertNotNull(result);
    assertSame(result.getValue(), envelope);
    assertFalse(result.isSanitized());

    fieldSpec.setMaxLength(VariantEnvelope.HEADER_SIZE);
    columnInfo = SanitizationTransformerUtils.getSanitizedColumnInfo(fieldSpec);
    SanitizationTransformerUtils.SanitizedColumnInfo finalColumnInfo = columnInfo;
    assertThrows(IllegalStateException.class,
        () -> SanitizationTransformerUtils.sanitizeValue(finalColumnInfo, envelope));
  }

  @Test
  public void testIngestionConversionValidatesPvarEnvelope() {
    byte[] envelope =
        VariantEnvelope.encode(ByteBuffer.wrap(new byte[]{1}), ByteBuffer.wrap(new byte[]{0}));
    assertSame(DataTypeTransformerUtils.transformValue(COLUMN, envelope, PinotDataType.VARIANT), envelope);
    assertThrows(IllegalArgumentException.class,
        () -> DataTypeTransformerUtils.transformValue(COLUMN, new byte[0], PinotDataType.VARIANT));
    assertThrows(IllegalArgumentException.class,
        () -> DataTypeTransformerUtils.transformValue(COLUMN, new byte[]{1, 2}, PinotDataType.VARIANT));
  }

  private static Schema schemaWith(FieldSpec fieldSpec) {
    return new Schema.SchemaBuilder().setSchemaName("variantSchema").addField(fieldSpec).build();
  }
}
