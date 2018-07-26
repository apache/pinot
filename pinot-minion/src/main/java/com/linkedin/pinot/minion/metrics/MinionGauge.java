/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.minion.metrics;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.metrics.AbstractMetrics;


public enum MinionGauge implements AbstractMetrics.Gauge {
  ;

  private final String _gaugeName;
  private final String _unit;
  private final boolean _global;

  MinionGauge(String unit, boolean global) {
    _gaugeName = Utils.toCamelCase(name().toLowerCase());
    _unit = unit;
    _global = global;
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
