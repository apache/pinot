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


/**
 * Window value aggregator for COUNT window function.
 */
public class CountWindowValueAggregator implements WindowValueAggregator<Object> {
  private long _count = 0;

  @Override
  public void addValue(@Nullable Object value) {
    if (value != null) {
      _count++;
    }
  }

  @Override
  public void removeValue(@Nullable Object value) {
    if (value != null) {
      _count--;
    }
  }

  @Override
  public Object getCurrentAggregatedValue() {
    return _count;
  }

  @Override
  public void clear() {
    _count = 0;
  }
}
