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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.roaringbitmap.RoaringBitmap;


/**
 * <code>MultiValueToSetTransformFunction</code> is the bridge for leaf stage Group By Multi-Value column.
 */
public class MultiValueToSetTransformFunction implements TransformFunction {
  private TransformFunction _transformFunction;
  private TransformResultMetadata _resultMetadata;

  @Override
  public String getName() {
    return "multiValueToSet";
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    if (arguments.size() != 1) {
      throw new IllegalArgumentException(
          "Expect exact 1 argument for MultiValueToSet operator [" + getName() + "], args [" + Arrays.toString(
              arguments.toArray()) + "].");
    }

    _transformFunction = arguments.get(0);
    _resultMetadata = _transformFunction.getResultMetadata();
    if (_resultMetadata.isSingleValue()) {
      throw new IllegalArgumentException(
          "Expect a multi-value column for MultiValueToSet operator [" + getName() + "], args [" + Arrays.toString(
              arguments.toArray()) + "].");
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return _transformFunction.getDictionary();
  }

  @Override
  public int[] transformToDictIdsSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException("Expected multi-value column for MultiValueToSet transform function");
  }

  @Override
  public int[][] transformToDictIdsMV(ValueBlock valueBlock) {
    return _transformFunction.transformToDictIdsMV(valueBlock);
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException("Expected multi-value column for MultiValueToSet transform function");
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException("Expected multi-value column for MultiValueToSet transform function");
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException("Expected multi-value column for MultiValueToSet transform function");
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException("Expected multi-value column for MultiValueToSet transform function");
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException("Expected multi-value column for MultiValueToSet transform function");
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException("Expected multi-value column for MultiValueToSet transform function");
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException("Expected multi-value column for MultiValueToSet transform function");
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    return _transformFunction.transformToIntValuesMV(valueBlock);
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    return _transformFunction.transformToLongValuesMV(valueBlock);
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    return _transformFunction.transformToFloatValuesMV(valueBlock);
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    return _transformFunction.transformToDoubleValuesMV(valueBlock);
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    return _transformFunction.transformToStringValuesMV(valueBlock);
  }

  @Override
  public byte[][][] transformToBytesValuesMV(ValueBlock valueBlock) {
    return _transformFunction.transformToBytesValuesMV(valueBlock);
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap(ValueBlock block) {
    return _transformFunction.getNullBitmap(block);
  }
}
