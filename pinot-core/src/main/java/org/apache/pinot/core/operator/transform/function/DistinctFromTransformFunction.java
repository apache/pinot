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
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.roaringbitmap.PeekableIntIterator;


/**
 * <code>DistinctFromTransformFunction</code> abstracts the transform needed for IsDistinctFrom and IsNotDistinctFrom.
 * Null value is considered as distinct from non-null value.
 * When both values are not null, this function calls equal transform function to determined whether two values are
 * distinct.
 * This function only supports two arguments which are both column names.
 */
public class DistinctFromTransformFunction extends BinaryOperatorTransformFunction {
  private int[] _results;
  // null iterator for left argument.
  private PeekableIntIterator _leftNullValueVectorIterator;
  // null iterator for right argument.
  private PeekableIntIterator _rightNullValueVectorIterator;
  // If _getDistinct is 1, act IsDistinctFrom transformation, otherwise act as IsNotDistinctFrom.
  private int _getDistinct;

  // getDistinct is set to true for IsDistinctFrom, otherwise it is for IsNotDistinctFrom.
  protected DistinctFromTransformFunction(boolean getDistinct) {
    super(getDistinct ? TransformFunctionType.NOT_EQUALS : TransformFunctionType.EQUALS);
    _getDistinct = getDistinct ? 1 : 0;
  }

  // Returns a bit vector of document length where each bit represents whether the row is null or not.
  // Set the value as not null by default if null option is disabled.
  static private BitSet getNullBits(@Nullable PeekableIntIterator nullValueVectorIterator, int length, int[] docIds) {
    BitSet nullBits = new BitSet(length);
    if (nullValueVectorIterator == null) {
      return nullBits;
    }
    int currentDocIdIndex = 0;
    while (nullValueVectorIterator.hasNext() & currentDocIdIndex < length) {
      nullValueVectorIterator.advanceIfNeeded(docIds[currentDocIdIndex]);
      currentDocIdIndex = Arrays.binarySearch(docIds, currentDocIdIndex, length, nullValueVectorIterator.next());
      if (currentDocIdIndex >= 0) {
        nullBits.set(currentDocIdIndex);
        currentDocIdIndex++;
      } else {
        currentDocIdIndex = -currentDocIdIndex - 1;
      }
    }
    return nullBits;
  }

  // Returns null iterator based on nullValueVectorIterator.
  // If null value is not enabled, which means nullValueVectorIterator can be null, this function will return a null.
  @Nullable
  static private PeekableIntIterator getNullValueIterator(Map<String, DataSource> dataSourceMap,
      TransformFunction transformFunction, PeekableIntIterator nullValueVectorIterator) {
    String columnName = ((IdentifierTransformFunction) transformFunction).getColumnName();
    NullValueVectorReader nullValueVectorReader = dataSourceMap.get(columnName).getNullValueVector();
    if (nullValueVectorIterator != null) {
      return nullValueVectorReader.getNullBitmap().getIntIterator();
    }
    return null;
  }

  @Override
  public String getName() {
    if (_getDistinct == 1) {
      return TransformFunctionType.IS_DISTINCT_FROM.getName();
    }
    return TransformFunctionType.IS_NOT_DISTINCT_FROM.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    super.init(arguments, dataSourceMap);
    if (!(_leftTransformFunction instanceof IdentifierTransformFunction)
        || !(_rightTransformFunction instanceof IdentifierTransformFunction)) {
      throw new IllegalArgumentException("Only column names are supported in DistinctFrom transformation.");
    }
    _leftNullValueVectorIterator =
        getNullValueIterator(dataSourceMap, _leftTransformFunction, _leftNullValueVectorIterator);
    _rightNullValueVectorIterator =
        getNullValueIterator(dataSourceMap, _rightTransformFunction, _rightNullValueVectorIterator);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_results == null || _results.length < length) {
      _results = new int[length];
    }
    _results = super.transformToIntValuesSV(projectionBlock);
    int[] docIds = projectionBlock.getDocIds();
    BitSet leftNull = getNullBits(_leftNullValueVectorIterator, length, docIds);
    BitSet rightNull = getNullBits(_rightNullValueVectorIterator, length, docIds);
    leftNull.xor(rightNull);
    // Iterate through docs that are null one side and not null the other side.
    for (int i = leftNull.nextSetBit(0); i >= 0; i = leftNull.nextSetBit(i + 1)) {
      _results[i] = _getDistinct;
    }
    // Iterate through docs that are null or  not null for both sides.
    for (int i = leftNull.nextClearBit(0); i >= 0; i = leftNull.nextSetBit(i + 1)) {
      // if both values are null, mark they are not distinct.
      if (rightNull.get(i)) {
        _results[i] = _getDistinct ^ 1;
      }
    }
    return _results;
  }
}
