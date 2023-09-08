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

import javax.annotation.Nullable;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.roaringbitmap.RoaringBitmap;


/**
 * The <code>AndOperatorTransformFunction</code> extends <code>LogicalOperatorTransformFunction</code> to
 * implement the logical operator 'AND'.
 *
 * The results are in boolean format and stored as an integer array with 1 represents true and 0 represents false.
 *
 * SQL Syntax:
 *    exprA AND exprB
 *
 */
public class AndOperatorTransformFunction extends LogicalOperatorTransformFunction {

  @Override
  public String getName() {
    return TransformFunctionType.AND.getName();
  }

  @Override
  int getLogicalFuncResult(int left, int right) {
    if ((left != 0) && (right != 0)) {
      return 1;
    }
    return 0;
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int numArguments = _arguments.size();
    int[][] intValuesSVs = new int[numArguments][];
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numArguments];
    RoaringBitmap nullBitmap = new RoaringBitmap();
    for (int i = 0; i < numArguments; i++) {
      intValuesSVs[i] = _arguments.get(i).transformToIntValuesSV(valueBlock);
      nullBitmaps[i] = _arguments.get(i).getNullBitmap(valueBlock);
    }
    for (int docId = 0; docId < numDocs; docId++) {
      boolean isFalse = false;
      for (int i = 0; i < numArguments; i++) {
        if ((nullBitmaps[i] == null || !nullBitmaps[i].contains(docId)) && intValuesSVs[i][docId] == 0) {
          isFalse = true;
          break;
        }
      }
      if (isFalse) {
        continue;
      }
      for (int i = 0; i < numArguments; i++) {
        if (nullBitmaps[i] != null && nullBitmaps[i].contains(docId)) {
          nullBitmap.add(docId);
          break;
        }
      }
    }
    return nullBitmap.isEmpty() ? null : nullBitmap;
  }
}
