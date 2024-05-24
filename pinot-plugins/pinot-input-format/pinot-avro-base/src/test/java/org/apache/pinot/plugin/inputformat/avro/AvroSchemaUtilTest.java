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
package org.apache.pinot.plugin.inputformat.avro;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroSchemaUtilTest {

  @Test
  public void testApplyLogicalTypeReturnsSameValueWhenFieldIsNull() {
    String value = "d7738003-1472-4f63-b0f1-b5e69c8b93e9";

    Object result = AvroSchemaUtil.applyLogicalType(null, value);

    Assert.assertTrue(result instanceof String);
    Assert.assertSame(value, result);
  }

  @Test
  public void testApplyLogicalTypeReturnsSameValueWhenNotUsingLogicalType() {
    String value = "abc";
    String schemaString =
        new StringBuilder().append("{").append("  \"type\": \"record\",").append("  \"name\": \"test\",")
            .append("  \"fields\": [{").append("    \"name\": \"column1\",").append("    \"type\": \"string\"")
            .append("  }]").append("}").toString();
    Schema schema = new Schema.Parser().parse(schemaString);

    Object result = AvroSchemaUtil.applyLogicalType(schema.getField("column1"), value);

    Assert.assertSame(value, result);
  }

  @Test
  public void testApplyLogicalTypeReturnsSameValueWhenNotConversionForLogicalTypeIsKnown() {
    String value = "abc";
    String schemaString =
        new StringBuilder().append("{").append("  \"type\": \"record\",").append("  \"name\": \"test\",")
            .append("  \"fields\": [{").append("    \"name\": \"column1\",").append("    \"type\": {")
            .append("      \"type\": \"bytes\",").append("      \"logicalType\": \"custom-type\"").append("    }")
            .append("  }]").append("}").toString();
    Schema schema = new Schema.Parser().parse(schemaString);

    Object result = AvroSchemaUtil.applyLogicalType(schema.getField("column1"), value);

    Assert.assertSame(value, result);
  }

  @Test
  public void testApplyLogicalTypeReturnsConvertedValueWhenConversionForLogicalTypeIsKnown() {
    String value = "d7738003-1472-4f63-b0f1-b5e69c8b93e9";
    String schemaString =
        new StringBuilder().append("{").append("  \"type\": \"record\",").append("  \"name\": \"test\",")
            .append("  \"fields\": [{").append("    \"name\": \"column1\",").append("    \"type\": {")
            .append("      \"type\": \"string\",").append("      \"logicalType\": \"uuid\"").append("    }")
            .append("  }]").append("}").toString();
    Schema schema = new Schema.Parser().parse(schemaString);

    Object result = AvroSchemaUtil.applyLogicalType(schema.getField("column1"), value);

    Assert.assertTrue(result instanceof UUID);
    Assert.assertEquals(UUID.fromString(value), result);
  }

  @Test
  public void testLogicalTypesWithUnionSchema() {
    String valString1 = "125.24350000";
    ByteBuffer amount1 = decimalToBytes(new BigDecimal(valString1), 8);
    // union schema for the logical field with "null" and "decimal" as types
    String fieldSchema1 = "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"amount\","
        + "\"type\":[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":8}]}]}";
    Schema schema1 = new Schema.Parser().parse(fieldSchema1);
    Object result1 = AvroSchemaUtil.applyLogicalType(schema1.getField("amount"), amount1);
    Assert.assertTrue(result1 instanceof BigDecimal);
    Assert.assertEquals(valString1, ((BigDecimal) result1).toPlainString());

    String valString2 = "125.53172";
    ByteBuffer amount2 = decimalToBytes(new BigDecimal(valString2), 5);
    // "null" not present within the union schema for the logical field amount
    String fieldSchema2 = "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"amount\","
        + "\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":5}]}]}";
    Schema schema2 = new Schema.Parser().parse(fieldSchema2);
    Object result2 = AvroSchemaUtil.applyLogicalType(schema2.getField("amount"), amount2);
    Assert.assertTrue(result2 instanceof BigDecimal);
    Assert.assertEquals(valString2, ((BigDecimal) result2).toPlainString());

    String valString3 = "211.53172864999";
    ByteBuffer amount3 = decimalToBytes(new BigDecimal(valString3), 11);
    // "null" present at the second position within the union schema for the logical field amount
    String fieldSchema3 = "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"amount\","
        + "\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":11}, \"null\"]}]}";
    Schema schema3 = new Schema.Parser().parse(fieldSchema3);
    Object result3 = AvroSchemaUtil.applyLogicalType(schema3.getField("amount"), amount3);
    Assert.assertTrue(result3 instanceof BigDecimal);
    Assert.assertEquals(valString3, ((BigDecimal) result3).toPlainString());
  }

  private static ByteBuffer decimalToBytes(BigDecimal decimal, int scale) {
    BigDecimal scaledValue = decimal.setScale(scale, RoundingMode.DOWN);
    byte[] unscaledBytes = scaledValue.unscaledValue().toByteArray();
    return ByteBuffer.wrap(unscaledBytes);
  }
}
