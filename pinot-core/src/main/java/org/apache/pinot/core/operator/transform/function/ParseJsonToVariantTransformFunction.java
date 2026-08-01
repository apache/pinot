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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.roaringbitmap.RoaringBitmap;


/// Parses JSON text into a logical Variant value for single-stage queries and ingestion transforms.
///
/// <p>Instances are query-local and not thread-safe.
public class ParseJsonToVariantTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "parseJson";
  private static final TransformResultMetadata RESULT_METADATA =
      new TransformResultMetadata(DataType.VARIANT, true, false);

  private final boolean _tolerant;
  private TransformFunction _jsonTransformFunction;
  private boolean _literalInput;
  @Nullable
  private byte[] _literalResult;
  @Nullable
  private ValueBlock _cachedValueBlock;
  @Nullable
  private RoaringBitmap _cachedNullBitmap;

  public ParseJsonToVariantTransformFunction() {
    this(false);
  }

  protected ParseJsonToVariantTransformFunction(boolean tolerant) {
    _tolerant = tolerant;
  }

  @Override
  public String getName() {
    return _tolerant ? Try.FUNCTION_NAME : FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap,
      boolean nullHandlingEnabled) {
    super.init(arguments, columnContextMap, nullHandlingEnabled);
    if (arguments.size() != 1) {
      throw new IllegalArgumentException(getName() + " expects exactly one argument");
    }
    _jsonTransformFunction = arguments.get(0);
    TransformResultMetadata inputMetadata = _jsonTransformFunction.getResultMetadata();
    boolean sqlNullLiteral =
        _jsonTransformFunction instanceof LiteralTransformFunction
            && ((LiteralTransformFunction) _jsonTransformFunction).isNull();
    if (!inputMetadata.isSingleValue()
        || (inputMetadata.getDataType().getStoredType() != DataType.STRING && !sqlNullLiteral)) {
      throw new IllegalArgumentException(getName() + " argument must be a single-value STRING or JSON");
    }
    _cachedValueBlock = null;
    _cachedNullBitmap = null;
    _literalInput = _jsonTransformFunction instanceof LiteralTransformFunction;
    if (_literalInput) {
      LiteralTransformFunction literal = (LiteralTransformFunction) _jsonTransformFunction;
      _literalResult = literal.isNull() ? null : parse(literal.getStringLiteral());
    } else {
      _literalResult = null;
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return RESULT_METADATA;
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    ensureParsed(valueBlock);
    return _cachedNullBitmap;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    ensureParsed(valueBlock);
    return _bytesValuesSV;
  }

  private void ensureParsed(ValueBlock valueBlock) {
    if (_cachedValueBlock == valueBlock) {
      return;
    }
    // Invalidate before mutating the reusable result buffer. A strict parse failure must not leave a previous block
    // marked as cached after its values have been partially overwritten.
    _cachedValueBlock = null;
    _cachedNullBitmap = null;
    int numDocs = valueBlock.getNumDocs();
    initBytesValuesSV(numDocs);
    if (_literalInput) {
      byte[] result = _literalResult;
      Arrays.fill(_bytesValuesSV, 0, numDocs, result != null ? result : NullValuePlaceHolder.BYTES);
      if (result == null && numDocs != 0) {
        RoaringBitmap resultNulls = new RoaringBitmap();
        resultNulls.add(0L, numDocs);
        _cachedNullBitmap = resultNulls;
      }
      _cachedValueBlock = valueBlock;
      return;
    }
    RoaringBitmap inputNulls = _jsonTransformFunction.getNullBitmap(valueBlock);
    RoaringBitmap resultNulls = inputNulls != null ? inputNulls.clone() : new RoaringBitmap();
    String[] jsonValues = _jsonTransformFunction.transformToStringValuesSV(valueBlock);
    for (int i = 0; i < numDocs; i++) {
      if (resultNulls.contains(i)) {
        _bytesValuesSV[i] = NullValuePlaceHolder.BYTES;
        continue;
      }
      byte[] value = parse(jsonValues[i]);
      if (value == null) {
        resultNulls.add(i);
        _bytesValuesSV[i] = NullValuePlaceHolder.BYTES;
      } else {
        _bytesValuesSV[i] = value;
      }
    }
    _cachedNullBitmap = resultNulls.isEmpty() ? null : resultNulls;
    _cachedValueBlock = valueBlock;
  }

  @Nullable
  private byte[] parse(@Nullable String json) {
    return _tolerant ? VariantUtils.tryParseJsonToVariant(json) : VariantUtils.parseJsonToVariant(json);
  }

  /// Tolerant JSON-to-Variant parsing.
  public static final class Try extends ParseJsonToVariantTransformFunction {
    public static final String FUNCTION_NAME = "tryParseJson";

    public Try() {
      super(true);
    }
  }
}
