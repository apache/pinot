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
import org.apache.pinot.common.function.TransformFunctionType;

/**
 * The <code>LeastTransformFunction</code> implements the Least operator.
 *
 * Return the smallest results for the arguments
 *
 * Expected result:
 * Least(columnA, columnB, columnC): smallest among columnA, columnB, columnC
 *
 * Note that null values will be ignored for evaluation. If all values are null, we return null.
 */
public class LeastTransformFunction extends SelectTupleElementTransformFunction {

  public LeastTransformFunction() {
    super(TransformFunctionType.LEAST.getName());
  }

  @Override
  protected int binaryFunction(int a, int b) {
    return Math.min(a, b);
  }

  @Override
  protected long binaryFunction(long a, long b) {
    return Math.min(a, b);
  }

  @Override
  protected float binaryFunction(float a, float b) {
    return Math.min(a, b);
  }

  @Override
  protected double binaryFunction(double a, double b) {
    return Math.min(a, b);
  }

  @Override
  protected BigDecimal binaryFunction(BigDecimal a, BigDecimal b) {
    return a.min(b);
  }

  @Override
  protected String binaryFunction(String a, String b) {
    if (a.compareTo(b) < 0) {
      return a;
    } else {
      return b;
    }
  }
}
