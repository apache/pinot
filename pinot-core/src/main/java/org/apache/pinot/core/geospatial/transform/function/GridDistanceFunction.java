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
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.spi.data.FieldSpec;

/**
 * Function that calculates the grid distance between two H3 indexes.
 * The function takes two arguments:
 *  - gridDistance(firstH3Index, secondH3Index)
 */
public class GridDistanceFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "gridDistance";
  private TransformFunction _firstArgument;
  private TransformFunction _secondArgument;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    Preconditions.checkArgument(arguments.size() == 2,
        "Transform function %s requires 2 arguments", getName());

    TransformFunction transformFunction = arguments.get(0);
    Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
        "First argument must be single-valued for transform function: %s", getName());
    _firstArgument = transformFunction;

    transformFunction = arguments.get(1);
    Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
        "Second argument must be single-valued for transform function: %s", getName());
    _secondArgument = transformFunction;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return new TransformResultMetadata(FieldSpec.DataType.LONG, true, false);
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initLongValuesSV(numDocs);

    long[] firstH3Indexes = _firstArgument.transformToLongValuesSV(valueBlock);
    long[] secondH3Indexes = _secondArgument.transformToLongValuesSV(valueBlock);

    for (int i = 0; i < numDocs; i++) {
      try {
        _longValuesSV[i] = ScalarFunctions.gridDistance(firstH3Indexes[i], secondH3Indexes[i]);
      } catch (Exception e) { }
    }

    return _longValuesSV;
  }
}
