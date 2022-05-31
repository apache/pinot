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

package org.apache.pinot.common.utils;

public class ExponentialMovingAverage {
  private double _alpha;
  private double _currAverage;

  public ExponentialMovingAverage(double alpha) {
    // TODO: Convert alpha to be based on time rather than number of values.
    _alpha = alpha;
    _currAverage = 0.0;
  }

  public double getLatency() {
    return _currAverage;
  }

  public double compute(double value) {
    if (_currAverage <= 0.0) {
      _currAverage = value;
      return _currAverage;
    }

    double newVal = value * _alpha + _currAverage * (1 - _alpha);
    _currAverage = newVal;
    return _currAverage;
  }
}
