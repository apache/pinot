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
package org.apache.pinot.spi.data;

import java.nio.ByteBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.VariantEnvelope;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class VariantFieldSpecTest {
  private static final byte[] METADATA = new byte[]{1, 2};
  private static final byte[] VALUE = new byte[]{3, 4, 5};

  @Test
  public void testStoredTypeAndDefaultNullValue() {
    assertEquals(DataType.VARIANT.getStoredType(), DataType.BYTES);
    assertFalse(DataType.VARIANT.isFixedWidth());
    assertFalse(DataType.VARIANT.supportsEquality());
    assertFalse(DataType.VARIANT.supportsHashing());
    assertFalse(DataType.VARIANT.supportsOrdering());
    assertFalse(DataType.VARIANT.supportsMinMax());
    assertFalse(DataType.VARIANT.supportsDirectAggregation());
    assertFalse(DataType.VARIANT.supportsPatternMatching());
    assertTrue(DataType.BYTES.supportsEquality());
    assertTrue(DataType.BYTES.supportsHashing());
    assertTrue(DataType.BYTES.supportsOrdering());
    assertTrue(DataType.BYTES.supportsMinMax());
    assertTrue(DataType.BYTES.supportsDirectAggregation());
    assertTrue(DataType.BYTES.supportsPatternMatching());
    assertFalse(DataType.MAP.supportsOrdering());
    assertFalse(DataType.MAP.supportsMinMax());

    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("payload", DataType.VARIANT, true);
    assertEquals((byte[]) fieldSpec.getDefaultNullValue(), new byte[0]);
    assertEquals(fieldSpec.getDefaultNullValueString(), "");
  }

  @Test
  public void testExternalAndInternalConversionValidateEnvelope() {
    byte[] envelope =
        VariantEnvelope.encode(ByteBuffer.wrap(METADATA), ByteBuffer.wrap(VALUE));
    String envelopeHex = BytesUtils.toHexString(envelope);

    assertEquals((byte[]) DataType.VARIANT.convert(envelopeHex), envelope);
    assertEquals(DataType.VARIANT.convertInternal(envelopeHex), new ByteArray(envelope));
    assertEquals(DataType.VARIANT.toString(envelope), envelopeHex);

    assertThrows(IllegalArgumentException.class, () -> DataType.VARIANT.convert(""));
    assertThrows(IllegalArgumentException.class, () -> DataType.VARIANT.convert("00"));
  }

  @Test
  public void testSemanticByteOperationsAreUnsupported() {
    byte[] envelope =
        VariantEnvelope.encode(ByteBuffer.wrap(METADATA), ByteBuffer.wrap(VALUE));

    assertThrows(UnsupportedOperationException.class, () -> DataType.VARIANT.equals(envelope, envelope));
    assertThrows(UnsupportedOperationException.class, () -> DataType.VARIANT.hashCode(envelope));
    assertThrows(UnsupportedOperationException.class, () -> DataType.VARIANT.compare(envelope, envelope));
  }

  @Test
  public void testSchemaStructuralEqualityAndHashCode()
      throws Exception {
    DimensionFieldSpec first = new DimensionFieldSpec("payload", DataType.VARIANT, true);
    DimensionFieldSpec equivalent = new DimensionFieldSpec("payload", DataType.VARIANT, true);
    DimensionFieldSpec different = new DimensionFieldSpec("otherPayload", DataType.VARIANT, true);

    assertEquals(first, equivalent);
    assertEquals(first.hashCode(), equivalent.hashCode());
    assertNotEquals(first, different);

    Schema firstSchema = new Schema.SchemaBuilder().setSchemaName("events").addField(first).build();
    Schema equivalentSchema =
        new Schema.SchemaBuilder().setSchemaName("events").addField(equivalent).build();
    assertEquals(firstSchema, equivalentSchema);
    assertEquals(firstSchema.hashCode(), equivalentSchema.hashCode());
    assertEquals(Schema.fromString(firstSchema.toString()), firstSchema);
  }

  @Test
  public void testVariantIsDimensionOnly() {
    Schema.validate(FieldType.DIMENSION, DataType.VARIANT);
    assertThrows(IllegalStateException.class, () -> Schema.validate(FieldType.METRIC, DataType.VARIANT));
    assertThrows(IllegalStateException.class, () -> Schema.validate(FieldType.TIME, DataType.VARIANT));
    assertThrows(IllegalStateException.class, () -> Schema.validate(FieldType.DATE_TIME, DataType.VARIANT));
    assertThrows(IllegalStateException.class, () -> Schema.validate(FieldType.COMPLEX, DataType.VARIANT));
  }
}
