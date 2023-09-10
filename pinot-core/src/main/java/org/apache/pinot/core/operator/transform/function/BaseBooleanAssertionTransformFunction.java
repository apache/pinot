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
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.roaringbitmap.RoaringBitmap;


public abstract class BaseBooleanAssertionTransformFunction extends BaseTransformFunction {
  private TransformFunction _transformFunction;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    Preconditions.checkArgument(arguments.size() == 1, "Exact 1 argument is required for " + getName());
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
    if (nullBitmap != null) {
      for (int docId = 0; docId < length; docId++) {
        if (nullBitmap.contains(docId)) {
          if (returnsTrueWhenValueIsNull()) {
            _intValuesSV[docId] = 1;
          }
        } else if (valueEvaluatesToTrue(intValuesSV[docId])) {
            _intValuesSV[docId] = 1;
        }
      }
    } else {
      for (int docId = 0; docId < length; docId++) {
        if (valueEvaluatesToTrue(intValuesSV[docId])) {
          _intValuesSV[docId] = 1;
        }
      }
    }
    return _intValuesSV;
  }

  protected abstract boolean returnsTrueWhenValueIsNull();

  protected abstract boolean valueEvaluatesToTrue(int value);

  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    return null;
  }
}
