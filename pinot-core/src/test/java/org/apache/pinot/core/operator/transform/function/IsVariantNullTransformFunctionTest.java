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
package org.apache.pinot.core.operator.transform.function;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;


public class IsVariantNullTransformFunctionTest {
  @Test
  public void testSqlNullReturnsNonNullFalse() {
    byte[] encodedVariantNull = VariantUtils.parseJsonToVariant("null");
    BytesTransformFunction input =
        new BytesTransformFunction(new byte[][]{encodedVariantNull}, RoaringBitmap.bitmapOf(0));
    IsVariantNullTransformFunction function = new IsVariantNullTransformFunction();
    function.init(List.of(input), Map.of(), true);
    ValueBlock block = valueBlock(1);

    assertEquals(function.getResultMetadata().getDataType(), DataType.BOOLEAN);
    assertEquals(function.transformToIntValuesSV(block)[0], 0,
        "The SQL-null bitmap must override any physical placeholder bytes");
    assertNull(function.getNullBitmap(block), "is_variant_null(SQL NULL) must be a non-null false");
  }

  @Test
  public void testLiteralSqlNullReturnsNonNullFalse() {
    IsVariantNullTransformFunction function = new IsVariantNullTransformFunction();
    function.init(
        List.of(new LiteralTransformFunction(new LiteralContext(DataType.UNKNOWN, null))), Map.of(), true);
    ValueBlock block = valueBlock(2);

    assertEquals(function.transformToIntValuesSV(block), new int[]{0, 0});
    assertNull(function.getNullBitmap(block));
  }

  @Test
  public void testRootAndPathVariantNullSemantics() {
    byte[] encodedVariantNull = VariantUtils.parseJsonToVariant("null");
    byte[] objectWithNull = VariantUtils.parseJsonToVariant("{\"coupon\":null}");
    byte[] objectWithMissingPath = VariantUtils.parseJsonToVariant("{\"eventType\":\"checkout\"}");
    BytesTransformFunction input =
        new BytesTransformFunction(new byte[][]{encodedVariantNull, objectWithNull, objectWithMissingPath}, null);
    ValueBlock block = valueBlock(3);

    IsVariantNullTransformFunction rootFunction = new IsVariantNullTransformFunction();
    rootFunction.init(List.of(input), Map.of(), true);
    int[] rootValues = rootFunction.transformToIntValuesSV(block);
    assertEquals(rootValues, new int[]{1, 0, 0});
    assertSame(rootFunction.transformToIntValuesSV(block), rootValues);
    assertNull(rootFunction.getNullBitmap(block));
    assertEquals(input._valueCallCount, 1);
    assertEquals(input._nullCallCount, 1);

    IsVariantNullTransformFunction pathFunction = new IsVariantNullTransformFunction();
    pathFunction.init(List.of(input, stringLiteral("$.coupon")), Map.of(), true);
    int[] pathValues = pathFunction.transformToIntValuesSV(block);
    assertEquals(pathValues, new int[]{0, 1, 0});
    assertSame(pathFunction.transformToIntValuesSV(block), pathValues);
    assertNull(pathFunction.getNullBitmap(block));
    assertEquals(input._valueCallCount, 2);
    assertEquals(input._nullCallCount, 2);
  }

  @Test
  public void testArgumentValidationAndFactoryRegistration() {
    BytesTransformFunction input =
        new BytesTransformFunction(new byte[][]{VariantUtils.parseJsonToVariant("true")}, null);
    IsVariantNullTransformFunction function = new IsVariantNullTransformFunction();

    assertThrows(IllegalArgumentException.class,
        () -> function.init(List.of(input, mock(TransformFunction.class)), Map.of(), true));
    assertThrows(IllegalArgumentException.class,
        () -> function.init(List.of(input, new LiteralTransformFunction(new LiteralContext(DataType.INT, 1))),
            Map.of(), true));
    assertThrows("An invalid path must fail during initialization, before any row is evaluated",
        IllegalArgumentException.class,
        () -> function.init(List.of(input, stringLiteral("coupon")), Map.of(), true));

    Map<String, Class<? extends TransformFunction>> functions = TransformFunctionFactory.getAllFunctions();
    assertSame(functions.get(TransformFunctionFactory.canonicalize("is_variant_null")),
        IsVariantNullTransformFunction.class);
  }

  private static LiteralTransformFunction stringLiteral(String value) {
    return new LiteralTransformFunction(new LiteralContext(DataType.STRING, value));
  }

  private static ValueBlock valueBlock(int numDocs) {
    ValueBlock valueBlock = mock(ValueBlock.class);
    when(valueBlock.getNumDocs()).thenReturn(numDocs);
    return valueBlock;
  }

  private static final class BytesTransformFunction extends BaseTransformFunction {
    private static final TransformResultMetadata RESULT_METADATA =
        new TransformResultMetadata(DataType.VARIANT, true, false);
    private final byte[][] _values;
    @Nullable
    private final RoaringBitmap _nullBitmap;
    private int _valueCallCount;
    private int _nullCallCount;

    private BytesTransformFunction(byte[][] values, @Nullable RoaringBitmap nullBitmap) {
      _values = values;
      _nullBitmap = nullBitmap;
    }

    @Override
    public String getName() {
      return "variantInput";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return RESULT_METADATA;
    }

    @Override
    public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
      _valueCallCount++;
      return _values;
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
      _nullCallCount++;
      return _nullBitmap;
    }
  }
}
