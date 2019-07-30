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

import java.math.BigInteger;


/**
 * An implementation of {@link AbstractAccumulator}, to count the score for {@link ParserBasedImpl}
 */
public class ParseBasedAccumulator extends AbstractAccumulator {
  private long _pureScore; //the appearance of a dimension as the top pick for a query
  private BigInteger _weightedScore; //the appearance weighted by numEntriesScannedInFilter

  public long getPureScore() {
    return _pureScore;
  }

  public BigInteger getWeightedScore() {
    return _weightedScore;
  }

  public ParseBasedAccumulator() {
    _pureScore = 0;
    _weightedScore = BigInteger.ZERO;
  }

  public void merge(int _pureScore, BigInteger _weightedScore) {
    super.increaseCount();
    this._pureScore += _pureScore;
    this._weightedScore = this._weightedScore.add(_weightedScore);
  }

  public void merge(ParseBasedAccumulator pb) {
    super.mergeCount(pb);
    this._pureScore += pb._pureScore;
    this._weightedScore = this._weightedScore.add(pb._weightedScore);
  }

  @Override
  public String toString() {
    return "ParseBasedMergerObj{" + "_pureScore=" + _pureScore + ", _weightedScore=" + _weightedScore + '}';
  }
}
