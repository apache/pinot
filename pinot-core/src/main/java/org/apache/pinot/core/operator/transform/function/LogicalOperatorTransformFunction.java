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
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.glassfish.jersey.internal.guava.Preconditions;
import org.roaringbitmap.RoaringBitmap;


/**
 * <code>LogicalOperatorTransformFunction</code> abstracts common functions for logical operators (AND, OR).
 * The results are BOOLEAN type.
 */
public abstract class LogicalOperatorTransformFunction extends BaseTransformFunction {
  protected List<TransformFunction> _arguments;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    _arguments = arguments;
    int numArguments = arguments.size();
    if (numArguments <= 1) {
      throw new IllegalArgumentException(
          "Expect more than 1 argument for logical operator [" + getName() + "], args [" + Arrays.toString(
              arguments.toArray()) + "].");
    }
    for (int i = 0; i < numArguments; i++) {
      TransformResultMetadata argumentMetadata = arguments.get(i).getResultMetadata();
      FieldSpec.DataType storedType = argumentMetadata.getDataType().getStoredType();
      Preconditions.checkState(
          argumentMetadata.isSingleValue() && storedType.isNumeric() || storedType.isUnknown(),
          "Unsupported argument type. Expecting single-valued boolean/number");
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initIntValuesSV(numDocs);
    System.arraycopy(_arguments.get(0).transformToIntValuesSV(valueBlock), 0, _intValuesSV, 0, numDocs);
    int numArguments = _arguments.size();
    for (int i = 1; i < numArguments; i++) {
      TransformFunction transformFunction = _arguments.get(i);
      int[] results = transformFunction.transformToIntValuesSV(valueBlock);
      for (int j = 0; j < numDocs; j++) {
        _intValuesSV[j] = getLogicalFuncResult(_intValuesSV[j], results[j]);
      }
    }
    return _intValuesSV;
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int numArguments = _arguments.size();
    RoaringBitmap nullBitmap = new RoaringBitmap();
    boolean[] supersedesNull = new boolean[numDocs];
    for (int i = 0; i < numArguments; i++) {
      int[] intValues = _arguments.get(i).transformToIntValuesSV(valueBlock);
      RoaringBitmap argumentNullBitmap = _arguments.get(i).getNullBitmap(valueBlock);
      for (int docId = 0; docId < numDocs; docId++) {
        if ((argumentNullBitmap == null || !argumentNullBitmap.contains(docId)) && valueSupersedesNull(
            intValues[docId])) {
          supersedesNull[docId] = true;
          nullBitmap.remove(docId);
        }
      }
      if (argumentNullBitmap != null) {
        for (int docId : argumentNullBitmap) {
          if (!supersedesNull[docId]) {
            nullBitmap.add(docId);
          }
        }
      }
    }
    return nullBitmap.isEmpty() ? null : nullBitmap;
  }

  abstract int getLogicalFuncResult(int left, int right);

  abstract boolean valueSupersedesNull(int i);
}
