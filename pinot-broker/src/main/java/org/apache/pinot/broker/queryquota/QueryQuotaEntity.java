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
package org.apache.pinot.broker.queryquota;

import com.google.common.util.concurrent.RateLimiter;


public class QueryQuotaEntity {

  private RateLimiter _rateLimiter;
  private HitCounter _hitCounterInSecond;
  private HitCounter _hitCounterInMinute;
  private int _numOnlineBrokers;
  private double _overallRate;
  private int _tableConfigStatVersion;

  public QueryQuotaEntity(RateLimiter rateLimiter, HitCounter hitCounterInSecond, HitCounter hitCounterInMinute,
      int numOnlineBrokers, double overallRate, int tableConfigStatVersion) {
    _rateLimiter = rateLimiter;
    _hitCounterInSecond = hitCounterInSecond;
    _hitCounterInMinute = hitCounterInMinute;
    _numOnlineBrokers = numOnlineBrokers;
    _overallRate = overallRate;
    _tableConfigStatVersion = tableConfigStatVersion;
  }

  public RateLimiter getRateLimiter() {
    return _rateLimiter;
  }

  public HitCounter getHitCounterInSecond() {
    return _hitCounterInSecond;
  }

  public HitCounter getHitCounterInMinute() {
    return _hitCounterInMinute;
  }

  public int getNumOnlineBrokers() {
    return _numOnlineBrokers;
  }

  public void setNumOnlineBrokers(int numOnlineBrokers) {
    _numOnlineBrokers = numOnlineBrokers;
  }

  public double getOverallRate() {
    return _overallRate;
  }

  public void setOverallRate(double overallRate) {
    _overallRate = overallRate;
  }

  public int getTableConfigStatVersion() {
    return _tableConfigStatVersion;
  }

  public void setTableConfigStatVersion(int tableConfigStatVersion) {
    _tableConfigStatVersion = tableConfigStatVersion;
  }
}
