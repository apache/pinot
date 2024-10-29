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


public interface WindowValueAggregator<T> {

  /**
   * Add a new value into the window aggregator.
   */
  void addValue(@Nullable T value);

  /**
   * Remove a value from the window aggregator. No validation is done to ensure that only a previously added value is
   * being removed. It is the responsibility of the caller to ensure that the value being removed is a valid one.
   */
  void removeValue(@Nullable T value);

  /**
   * Get the current aggregated value for the window.
   */
  @Nullable
  T getCurrentAggregatedValue();

  /**
   * Remove all values from the window aggregator.
   */
  void clear();
}
