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
package org.apache.pinot.query.runtime.operator.window.aggregate;

import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.BooleanUtils;


/**
 * Window value aggregator for BOOL_OR window function.
 */
public class BoolOrValueAggregator implements WindowValueAggregator<Object> {
  private int _numFalse = 0;
  private int _numTrue = 0;
  private int _numNull = 0;

  @Override
  public void addValue(@Nullable Object value) {
    if (value == null) {
      _numNull++;
    } else if (BooleanUtils.isFalseInternalValue(value)) {
      _numFalse++;
    } else {
      _numTrue++;
    }
  }

  @Override
  public void removeValue(@Nullable Object value) {
    if (value == null) {
      _numNull--;
    } else if (BooleanUtils.isFalseInternalValue(value)) {
      _numFalse--;
    } else {
      _numTrue--;
    }
  }

  @Nullable
  @Override
  public Object getCurrentAggregatedValue() {
    if (_numTrue > 0) {
      return BooleanUtils.INTERNAL_TRUE;
    }
    if (_numNull > 0) {
      return null;
    }
    if (_numFalse > 0) {
      return BooleanUtils.INTERNAL_FALSE;
    }
    return null;
  }

  @Override
  public void clear() {
    _numFalse = 0;
    _numTrue = 0;
    _numNull = 0;
  }
}
