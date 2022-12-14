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
package org.apache.pinot.spi.metrics;

import org.apache.pinot.spi.utils.CommonUtils;


public enum StreamConsumerGauge implements AbstractMetrics.Gauge {
  PARITTION_RECORDS_LAG("count", false),
  RECEIVED_RECORDS("count", false);

  private final String _gaugeName;
  private final String _unit;
  private final boolean _global;

  StreamConsumerGauge(String unit, boolean global) {
    _unit = unit;
    _global = global;
    _gaugeName = CommonUtils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getGaugeName() {
    return _gaugeName;
  }

  @Override
  public String getUnit() {
    return _unit;
  }

  @Override
  public boolean isGlobal() {
    return _global;
  }
}
