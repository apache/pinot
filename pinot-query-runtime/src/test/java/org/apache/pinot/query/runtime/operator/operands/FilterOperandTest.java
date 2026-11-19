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
package org.apache.pinot.query.runtime.operator.operands;

import java.io.IOException;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class FilterOperandTest {
  private static final DataSchema INT_SCHEMA =
      new DataSchema(new String[]{"value"}, new ColumnDataType[]{ColumnDataType.INT});
  private static final DataSchema VARIANT_SCHEMA =
      new DataSchema(new String[]{"payload"}, new ColumnDataType[]{ColumnDataType.VARIANT});
  private static final DataSchema OBJECT_SCHEMA =
      new DataSchema(new String[]{"payload"}, new ColumnDataType[]{ColumnDataType.OBJECT});
  private static final RexExpression NULL_LITERAL = new RexExpression.Literal(ColumnDataType.UNKNOWN, null);
  private static final List<RexExpression> VARIANT_OPERANDS =
      List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(0));

  @Test
  public void testComparisonWithNullLiteral() {
    FilterOperand.Predicate leftNull = new FilterOperand.Predicate(
        List.of(NULL_LITERAL, new RexExpression.InputRef(0)), INT_SCHEMA, value -> value == 0);
    Assert.assertNull(leftNull.apply(List.of(1)));

    FilterOperand.Predicate rightNull = new FilterOperand.Predicate(
        List.of(new RexExpression.InputRef(0), NULL_LITERAL), INT_SCHEMA, value -> value == 0);
    Assert.assertNull(rightNull.apply(List.of(1)));
  }

  @Test
  public void testRawVariantComparisonIsRejected() {
    IllegalArgumentException exception =
        Assert.expectThrows(IllegalArgumentException.class,
            () -> new FilterOperand.Predicate(VARIANT_OPERANDS, VARIANT_SCHEMA, value -> value == 0));
    Assert.assertTrue(exception.getMessage().contains("Raw VARIANT values do not support comparison"));
  }

  @Test
  public void testRawVariantInIsRejected() {
    IllegalArgumentException exception =
        Assert.expectThrows(IllegalArgumentException.class,
            () -> new FilterOperand.In(VARIANT_OPERANDS, VARIANT_SCHEMA, false));
    Assert.assertTrue(exception.getMessage().contains("Raw VARIANT values do not support IN"));
  }

  @Test
  public void testRawVariantNotInIsRejected() {
    IllegalArgumentException exception =
        Assert.expectThrows(IllegalArgumentException.class,
            () -> new FilterOperand.In(VARIANT_OPERANDS, VARIANT_SCHEMA, true));
    Assert.assertTrue(exception.getMessage().contains("Raw VARIANT values do not support IN"));
  }

  @Test
  public void testInIdSetRejectsRawVariantButAllowsBytes()
      throws IOException {
    byte[] value = VariantUtils.parseJsonToVariant("{\"id\":1}");
    IdSet idSet = IdSets.create(DataType.BYTES, 0, 100, 0.0001);
    idSet.add(value);
    List<RexExpression> operands = List.of(new RexExpression.InputRef(0),
        new RexExpression.Literal(ColumnDataType.STRING, idSet.toBase64String()));
    DataSchema bytesSchema = new DataSchema(new String[]{"payload"}, new ColumnDataType[]{ColumnDataType.BYTES});
    for (String functionName : List.of("IN_ID_SET", "inIdSet")) {
      RexExpression.FunctionCall functionCall =
          new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, functionName, operands);
      assertEquals(TransformOperandFactory.getTransformOperand(functionCall, bytesSchema)
          .apply(List.of(new ByteArray(value))), 1);
      IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
          () -> TransformOperandFactory.getTransformOperand(functionCall, VARIANT_SCHEMA));
      assertTrue(exception.getMessage().contains("Raw VARIANT values do not support IN"));
    }
  }

  @Test
  public void testNonVariantOpaqueTypesAreNotRejectedAsVariant() {
    // OBJECT (and other non-orderable types) must keep their existing best-effort comparison/IN behavior rather
    // than being rejected with the VARIANT-specific guard. Constructing the operands must not throw.
    new FilterOperand.Predicate(VARIANT_OPERANDS, OBJECT_SCHEMA, value -> value == 0);
    new FilterOperand.In(VARIANT_OPERANDS, OBJECT_SCHEMA, false);
    for (String functionName : List.of("IS_DISTINCT_FROM", "isNotDistinctFrom")) {
      RexExpression.FunctionCall functionCall =
          new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, functionName, VARIANT_OPERANDS);
      TransformOperandFactory.getTransformOperand(functionCall, OBJECT_SCHEMA);
    }
  }

  @Test
  public void testRawVariantDistinctFromIsRejected() {
    for (String functionName : List.of("IS_DISTINCT_FROM", "isNotDistinctFrom")) {
      RexExpression.FunctionCall functionCall =
          new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, functionName, VARIANT_OPERANDS);
      IllegalArgumentException exception = Assert.expectThrows(IllegalArgumentException.class,
          () -> TransformOperandFactory.getTransformOperand(functionCall, VARIANT_SCHEMA));
      Assert.assertTrue(exception.getMessage().contains("Raw VARIANT values do not support comparison"));
    }
  }
}
