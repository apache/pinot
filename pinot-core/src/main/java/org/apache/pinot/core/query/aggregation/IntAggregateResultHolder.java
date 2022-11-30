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

package org.apache.pinot.core.query.aggregation;

public class IntAggregateResultHolder implements AggregationResultHolder {

  int _value;

  public IntAggregateResultHolder(int defaultValue) {
    _value = defaultValue;
  }

  @Override
  public void setValue(double value) {
    throw new RuntimeException("Method 'setValue' (with double value) not supported for class " + getClass().getName());
  }

  @Override
  public void setValue(int value) {
    _value = value;
  }

  @Override
  public void setValue(Object value) {
    throw new RuntimeException("Method 'setValue' (with object value) not supported for class " + getClass().getName());
  }

  @Override
  public double getDoubleResult() {
    throw new RuntimeException("Method 'getDoubleResult' not supported for class " + getClass().getName());
  }

  @Override
  public int getIntResult() {
    return _value;
  }

  @Override
  public <T> T getResult() {
    throw new RuntimeException("Method 'getResult' not supported for class " + getClass().getName());
  }
}
