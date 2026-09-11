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
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class VariantToJsonTransformFunctionTest {

  /// The three-way null contract: SQL null renders as SQL null, an encoded Variant null renders as JSON text
  /// `null`, and the Variant string "null" renders quoted.
  @Test
  public void testRendersNullStatesDistinctly() {
    byte[] variantNull = VariantUtils.parseJsonToVariant("null");
    byte[] nullString = VariantUtils.parseJsonToVariant("\"null\"");
    byte[] object = VariantUtils.parseJsonToVariant("{\"name\":\"alice\",\"score\":3}");
    RoaringBitmap inputNulls = RoaringBitmap.bitmapOf(0);
    BytesTransformFunction input =
        new BytesTransformFunction(new byte[][]{new byte[0], variantNull, nullString, object}, inputNulls);

    VariantToJsonTransformFunction function = new VariantToJsonTransformFunction();
    function.init(List.of(input), Map.of(), true);
    ValueBlock block = valueBlock(4);

    assertEquals(function.getResultMetadata().getDataType(), DataType.STRING);
    String[] values = function.transformToStringValuesSV(block);
    assertEquals(values[1], "null");
    assertEquals(values[2], "\"null\"");
    assertEquals(values[3], "{\"name\":\"alice\",\"score\":3}");
    assertEquals(function.getNullBitmap(block), RoaringBitmap.bitmapOf(0));
  }

  @Test
  public void testRejectsExtraArguments() {
    byte[] object = VariantUtils.parseJsonToVariant("{}");
    BytesTransformFunction input = new BytesTransformFunction(new byte[][]{object}, null);
    VariantToJsonTransformFunction function = new VariantToJsonTransformFunction();
    assertThrows(IllegalArgumentException.class,
        () -> function.init(List.of(input, input), Map.of(), true));
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
      return _values;
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
      return _nullBitmap;
    }
  }
}
