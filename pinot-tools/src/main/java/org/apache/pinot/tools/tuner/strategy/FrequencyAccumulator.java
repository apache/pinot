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

import javax.annotation.concurrent.NotThreadSafe;


/**
 * An implementation of {@link AbstractAccumulator}, count the frequency of dimension appearance for {@link FrequencyImpl}
 */
@NotThreadSafe
public class FrequencyAccumulator extends AbstractAccumulator {

  public FrequencyAccumulator() {
    _frequency = 0;
  }

  private long _frequency;

  public long getFrequency() {
    return _frequency;
  }

  public void incrementFrequency() {
    super.increaseCount();
    this._frequency += 1;
  }

  public void merge(FrequencyAccumulator fobj) {
    super.mergeCount(fobj);
    this._frequency += fobj._frequency;
  }

  @Override
  public String toString() {
    return "ParseBasedMergerObj{" + "_frequency=" + _frequency + '}';
  }
}
