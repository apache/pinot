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
}
