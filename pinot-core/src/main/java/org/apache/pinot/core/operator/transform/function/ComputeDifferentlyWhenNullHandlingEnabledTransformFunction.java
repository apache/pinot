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
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


/**
 * Base class for transform functions that compute differently (using value and NULL together) when NULL handling is
 * enabled.
 */
public abstract class ComputeDifferentlyWhenNullHandlingEnabledTransformFunction extends BaseTransformFunction {

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    if (getResultMetadata().getDataType().getStoredType() != FieldSpec.DataType.INT) {
      return super.transformToIntValuesSV(valueBlock);
    }
    if (_nullHandlingEnabled) {
      return transformToIntValuesSVUsingValueAndNull(valueBlock);
    } else {
      return transformToIntValuesSVUsingValue(valueBlock);
    }
  }

  protected int[] transformToIntValuesSVUsingValue(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  protected int[] transformToIntValuesSVUsingValueAndNull(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    if (getResultMetadata().getDataType().getStoredType() != FieldSpec.DataType.LONG) {
      return super.transformToLongValuesSV(valueBlock);
    }
    if (_nullHandlingEnabled) {
      return transformToLongValuesSVUsingValueAndNull(valueBlock);
    } else {
      return transformToLongValuesSVUsingValue(valueBlock);
    }
  }

  protected long[] transformToLongValuesSVUsingValue(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  protected long[] transformToLongValuesSVUsingValueAndNull(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    if (getResultMetadata().getDataType().getStoredType() != FieldSpec.DataType.FLOAT) {
      return super.transformToFloatValuesSV(valueBlock);
    }
    if (_nullHandlingEnabled) {
      return transformToFloatValuesSVUsingValueAndNull(valueBlock);
    } else {
      return transformToFloatValuesSVUsingValue(valueBlock);
    }
  }

  protected float[] transformToFloatValuesSVUsingValue(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  protected float[] transformToFloatValuesSVUsingValueAndNull(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    if (getResultMetadata().getDataType().getStoredType() != FieldSpec.DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(valueBlock);
    }
    if (_nullHandlingEnabled) {
      return transformToDoubleValuesSVUsingValueAndNull(valueBlock);
    } else {
      return transformToDoubleValuesSVUsingValue(valueBlock);
    }
  }

  protected double[] transformToDoubleValuesSVUsingValue(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  protected double[] transformToDoubleValuesSVUsingValueAndNull(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    if (getResultMetadata().getDataType().getStoredType() != FieldSpec.DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSV(valueBlock);
    }
    if (_nullHandlingEnabled) {
      return transformToBigDecimalValuesSVUsingValueAndNull(valueBlock);
    } else {
      return transformToBigDecimalValuesSVUsingValue(valueBlock);
    }
  }

  protected BigDecimal[] transformToBigDecimalValuesSVUsingValue(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  protected BigDecimal[] transformToBigDecimalValuesSVUsingValueAndNull(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    if (getResultMetadata().getDataType().getStoredType() != FieldSpec.DataType.STRING) {
      return super.transformToStringValuesSV(valueBlock);
    }
    if (_nullHandlingEnabled) {
      return transformToStringValuesSVUsingValueAndNull(valueBlock);
    } else {
      return transformToStringValuesSVUsingValue(valueBlock);
    }
  }

  protected String[] transformToStringValuesSVUsingValue(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  protected String[] transformToStringValuesSVUsingValueAndNull(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    if (getResultMetadata().getDataType().getStoredType() != FieldSpec.DataType.BYTES) {
      return super.transformToBytesValuesSV(valueBlock);
    }
    if (_nullHandlingEnabled) {
      return transformToBytesValuesSVUsingValueAndNull(valueBlock);
    } else {
      return transformToBytesValuesSVUsingValue(valueBlock);
    }
  }

  protected byte[][] transformToBytesValuesSVUsingValue(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  protected byte[][] transformToBytesValuesSVUsingValueAndNull(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public abstract RoaringBitmap getNullBitmap(ValueBlock valueBlock);
}
