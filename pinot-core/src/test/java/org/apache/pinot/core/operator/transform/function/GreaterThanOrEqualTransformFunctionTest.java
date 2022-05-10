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


public class GreaterThanOrEqualTransformFunctionTest extends BinaryOperatorTransformFunctionTest {

  @Override
  int getExpectedValue(int value, int toCompare) {
    return (value >= toCompare) ? 1 : 0;
  }

  @Override
  int getExpectedValue(long value, long toCompare) {
    return (value >= toCompare) ? 1 : 0;
  }

  @Override
  int getExpectedValue(float value, float toCompare) {
    return (value >= toCompare) ? 1 : 0;
  }

  @Override
  int getExpectedValue(double value, double toCompare) {
    return (value >= toCompare) ? 1 : 0;
  }

  @Override
  int getExpectedValue(BigDecimal value, BigDecimal toCompare) {
    return value.compareTo(toCompare) >= 0 ? 1 : 0;
  }

  @Override
  int getExpectedValue(String value, String toCompare) {
    return (value.compareTo(toCompare) >= 0) ? 1 : 0;
  }

  @Override
  String getFuncName() {
    return new GreaterThanOrEqualTransformFunction().getName();
  }
}
