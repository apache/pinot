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

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.common.utils.VariantUtils.ResultType;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;


/**
 * Single-stage typed extraction from a Variant envelope.
 *
 * <p>The path and optional target type must be literals, so they are parsed once during initialization. Omitting the
 * target type returns a Variant. A missing path returns SQL null; the strict form throws for an incompatible non-null
 * value, while {@link Try} maps that failure to SQL null. Instances are query-local and not thread-safe.
 */
public class VariantGetTransformFunction extends BaseVariantTransformFunction {
  public static final String FUNCTION_NAME = "variantGet";
  private final boolean _tolerant;
  private ResultType _targetType;
  private DataType _storedType;
  private TransformResultMetadata _resultMetadata;

  public VariantGetTransformFunction() {
    this(false);
  }

  protected VariantGetTransformFunction(boolean tolerant) {
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
    initVariantArguments(arguments, 2, 3, true);
    int numArguments = arguments.size();
    if (numArguments == 3 && (!(arguments.get(2) instanceof LiteralTransformFunction)
        || arguments.get(2).getResultMetadata().getDataType() != DataType.STRING)) {
      throw new IllegalArgumentException(getName() + " target type must be a string literal");
    }
    _targetType = numArguments == 2 ? ResultType.VARIANT
        : VariantUtils.parseResultType(((LiteralTransformFunction) arguments.get(2)).getStringLiteral());
    DataType resultType = _targetType.getDataType();
    _storedType = resultType.getStoredType();
    _resultMetadata = new TransformResultMetadata(resultType, true, false);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    if (_storedType != DataType.INT) {
      return super.transformToIntValuesSV(valueBlock);
    }
    ensureEvaluated(valueBlock);
    return _intValuesSV;
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    if (_storedType != DataType.LONG) {
      return super.transformToLongValuesSV(valueBlock);
    }
    ensureEvaluated(valueBlock);
    return _longValuesSV;
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    if (_storedType != DataType.FLOAT) {
      return super.transformToFloatValuesSV(valueBlock);
    }
    ensureEvaluated(valueBlock);
    return _floatValuesSV;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    if (_storedType != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(valueBlock);
    }
    ensureEvaluated(valueBlock);
    return _doubleValuesSV;
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    if (_storedType != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSV(valueBlock);
    }
    ensureEvaluated(valueBlock);
    return _bigDecimalValuesSV;
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    if (_storedType != DataType.STRING) {
      return super.transformToStringValuesSV(valueBlock);
    }
    ensureEvaluated(valueBlock);
    return _stringValuesSV;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    if (_storedType != DataType.BYTES) {
      return super.transformToBytesValuesSV(valueBlock);
    }
    ensureEvaluated(valueBlock);
    return _bytesValuesSV;
  }

  @Override
  protected void initResultValues(int numDocs) {
    switch (_storedType) {
      case INT:
        initIntValuesSV(numDocs);
        break;
      case LONG:
        initLongValuesSV(numDocs);
        break;
      case FLOAT:
        initFloatValuesSV(numDocs);
        break;
      case DOUBLE:
        initDoubleValuesSV(numDocs);
        break;
      case BIG_DECIMAL:
        initBigDecimalValuesSV(numDocs);
        break;
      case STRING:
        initStringValuesSV(numDocs);
        break;
      case BYTES:
        initBytesValuesSV(numDocs);
        break;
      default:
        throw new IllegalStateException("Unsupported Variant result storage type: " + _storedType);
    }
  }

  @Override
  protected boolean evaluateVariant(@Nullable byte[] variant, int index) {
    boolean extracted = _tolerant
        ? VariantUtils.tryExtractInto(variant, getVariantPath(), _targetType, getReusableResult())
        : VariantUtils.extractInto(variant, getVariantPath(), _targetType, getReusableResult());
    if (extracted) {
      setExtractedValue(index);
    }
    return extracted;
  }

  @Override
  protected void setNullValue(int index) {
    switch (_storedType) {
      case INT:
        _intValuesSV[index] = NullValuePlaceHolder.INT;
        break;
      case LONG:
        _longValuesSV[index] = NullValuePlaceHolder.LONG;
        break;
      case FLOAT:
        _floatValuesSV[index] = NullValuePlaceHolder.FLOAT;
        break;
      case DOUBLE:
        _doubleValuesSV[index] = NullValuePlaceHolder.DOUBLE;
        break;
      case BIG_DECIMAL:
        _bigDecimalValuesSV[index] = NullValuePlaceHolder.BIG_DECIMAL;
        break;
      case STRING:
        _stringValuesSV[index] = NullValuePlaceHolder.STRING;
        break;
      case BYTES:
        _bytesValuesSV[index] = NullValuePlaceHolder.BYTES;
        break;
      default:
        throw new IllegalStateException("Unsupported Variant result storage type: " + _storedType);
    }
  }

  private void setExtractedValue(int index) {
    switch (_storedType) {
      case INT:
        _intValuesSV[index] = getReusableResult().getIntValue();
        break;
      case LONG:
        _longValuesSV[index] = getReusableResult().getLongValue();
        break;
      case FLOAT:
        _floatValuesSV[index] = getReusableResult().getFloatValue();
        break;
      case DOUBLE:
        _doubleValuesSV[index] = getReusableResult().getDoubleValue();
        break;
      case BIG_DECIMAL:
        _bigDecimalValuesSV[index] = getReusableResult().getBigDecimalValue();
        break;
      case STRING:
        _stringValuesSV[index] = getReusableResult().getStringValue();
        break;
      case BYTES:
        _bytesValuesSV[index] = getReusableResult().getBytesValue();
        break;
      default:
        throw new IllegalStateException("Unsupported Variant result storage type: " + _storedType);
    }
  }

  /**
   * Tolerant Variant extraction.
   */
  public static final class Try extends VariantGetTransformFunction {
    public static final String FUNCTION_NAME = "tryVariantGet";

    public Try() {
      super(true);
    }
  }
}
