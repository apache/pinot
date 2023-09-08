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

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.roaringbitmap.RoaringBitmap;


public class IsTrueTransformFunction extends BaseTransformFunction {
  private TransformFunction _transformFunction;

  @Override
  public String getName() {
    return TransformFunctionType.IS_TRUE.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    Preconditions.checkArgument(arguments.size() == 1, "Exact 1 argument is required for IS_NULL");
    _transformFunction = arguments.get(0);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initIntValuesSV(length);
    Arrays.fill(_intValuesSV, 0);
    int[] intValuesSV = _transformFunction.transformToIntValuesSV(valueBlock);
    RoaringBitmap nullBitmap = null;
    if (_nullHandlingEnabled) {
      nullBitmap = _transformFunction.getNullBitmap(valueBlock);
    }
    for (int docId = 0; docId < length; docId++) {
      if (returnsTrue(intValuesSV[docId]) && (nullBitmap == null || !nullBitmap.contains(docId))) {
        _intValuesSV[docId] = 1;
      }
    }
    return _intValuesSV;
  }

  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    return null;
  }

  protected boolean returnsTrue(int val) {
    return val != 0;
  }
}
