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
package org.apache.pinot.core.geospatial.transform.function;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;

/**
 * Function that returns all H3 indexes within a specified hexagonal grid distance from a given origin index.
 * The function takes two arguments:
 *  - gridDisk(origin, k)
 */
public class GridDiskFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "gridDisk";
  private TransformFunction _originArgument;
  private TransformFunction _kArgument;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    Preconditions.checkArgument(arguments.size() == 2, "Transform function %s requires 2 arguments", getName());

    TransformFunction transformFunction = arguments.get(0);
    Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
        "First argument (origin) must be single-valued for transform function: %s", getName());
    _originArgument = transformFunction;

    transformFunction = arguments.get(1);
    Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
        "Second argument (k) must be single-valued for transform function: %s", getName());
    _kArgument = transformFunction;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    // Returns a list of H3 indexes, so it's multi-valued
    return new TransformResultMetadata(FieldSpec.DataType.LONG, false, false);
  }

  @Nullable
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    return null;
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initLongValuesMV(numDocs);

    long[] origins = _originArgument.transformToLongValuesSV(valueBlock);
    int[] ks = _kArgument.transformToIntValuesSV(valueBlock);

    for (int i = 0; i < numDocs; i++) {
      List<Long> diskIndexes = ScalarFunctions.gridDisk(origins[i], ks[i]);
      _longValuesMV[i] = diskIndexes.stream().mapToLong(Long::longValue).toArray();
    }

    return _longValuesMV;
  }
}
