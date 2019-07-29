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
package org.apache.pinot.tools.tuner.strategy;

/**
 * Accumulator for column stats
 */
public abstract class AbstractAccumulator {
  public abstract String toString();

  /**
   * Get the default counter for BasicMergerObjs merged to this AbstractMergerObj
   * @return
   */
  public long getCount() {
    return _count;
  }

  private long _count = 0;

  /**
   * Increase default counter by one
   */
  public void increaseCount() {
    this._count += 1;
  }

  /**
   * Merge the default counter of two BasicMergerObjs
   * @param abstractAccumulator AbstractMergerObj to merge to this AbstractMergerObj
   */
  public void mergeCount(AbstractAccumulator abstractAccumulator) {
    this._count += abstractAccumulator._count;
  }
}
