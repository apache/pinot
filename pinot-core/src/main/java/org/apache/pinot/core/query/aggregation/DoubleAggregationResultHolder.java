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

// TODO DDC one for BigDecimal?
/**
 * AggregationResultHolder interface implementation for result type 'primitive double'.
 *
 */
public class DoubleAggregationResultHolder implements AggregationResultHolder {
  double _value;

  /**
   * Constructor for the class.
   * @param defaultValue
   */
  public DoubleAggregationResultHolder(double defaultValue) {
    _value = defaultValue;
  }

  /**
   * {@inheritDoc}
   * @param value
   */
  @Override
  public void setValue(double value) {
    _value = value;
  }

  /**
   * {@inheritDoc}
   * Value for this class is 'primitive double', so this method is not implemented.
   * @param value
   */
  @Override
  public void setValue(Object value) {
    throw new RuntimeException("Method 'setValue' (with object value) not supported for class " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public double getDoubleResult() {
    return _value;
  }

  /**
   * {@inheritDoc}
   * Result for this class is 'primitive double', so this method is not implemented.
   * @return
   */
  @Override
  public <T> T getResult() {
    throw new RuntimeException("Method 'getResult' not supported for class " + getClass().getName());
  }
}
