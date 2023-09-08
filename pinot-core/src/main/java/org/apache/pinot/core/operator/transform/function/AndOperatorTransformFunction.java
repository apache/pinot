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

import org.apache.pinot.common.function.TransformFunctionType;


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

  private boolean isFalse(int i) {
    return i == 0;
  }

  @Override
  boolean valueSupersedesNull(int i) {
    return isFalse(i);
  }
}
