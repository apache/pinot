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
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.roaringbitmap.RoaringBitmap;


/**
 * <code>DistinctFromTransformFunction</code> abstracts the transform needed for IsDistinctFrom and IsNotDistinctFrom.
 * Null value is considered as distinct from non-null value.
 * When both values are not null, this function calls equal transform function to determined whether two values are
 * distinct.
 * This function only supports two arguments which are both column names.
 */
public class DistinctFromTransformFunction extends BinaryOperatorTransformFunction {
  // If _getDistinct is true, act as IsDistinctFrom transformation, otherwise act as IsNotDistinctFrom.
  private boolean _getDistinct;

  /**
   * @param getDistinct is set to true for IsDistinctFrom, otherwise it is for IsNotDistinctFrom.
   */
  protected DistinctFromTransformFunction(boolean getDistinct) {
    super(getDistinct ? TransformFunctionType.NOT_EQUALS : TransformFunctionType.EQUALS);
    _getDistinct = getDistinct;
  }

  /**
   * Returns a bit map of corresponding column.
   * Returns an empty bitmap by default if null option is disabled.
   */
  private static RoaringBitmap getNullBitMap(ProjectionBlock projectionBlock, TransformFunction transformFunction) {
    String columnName = ((IdentifierTransformFunction) transformFunction).getColumnName();
    RoaringBitmap nullBitmap = projectionBlock.getBlockValueSet(columnName).getNullBitmap();
    if (nullBitmap == null) {
      return new RoaringBitmap();
    }
    return nullBitmap;
  }

  @Override
  public String getName() {
    if (_getDistinct) {
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
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    _results = super.transformToIntValuesSV(projectionBlock);
    RoaringBitmap letNull = getNullBitMap(projectionBlock, _leftTransformFunction);
    RoaringBitmap rightNull = getNullBitMap(projectionBlock, _rightTransformFunction);
    RoaringBitmap xorNull = RoaringBitmap.xor(letNull, rightNull);
    for (int i = 0; i < length; i++) {
      if (xorNull.contains(i)) {
        // These are docs that are null one side and not null the other side.
        _results[i] = _getDistinct ? 1 : 0;
      } else if (rightNull.contains(i)) {
        // If both values are null, mark they are not distinct.
        _results[i] = _getDistinct ? 0 : 1;
      }
    }
    return _results;
  }
}
