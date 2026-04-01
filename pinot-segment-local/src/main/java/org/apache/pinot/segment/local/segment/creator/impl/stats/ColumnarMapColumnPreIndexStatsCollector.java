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
package org.apache.pinot.segment.local.segment.creator.impl.stats;

import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;


/**
 * Statistics collector for MAP columns with sparse map index.
 *
 * <p>Unlike regular columns, MAP columns with sparse map index store all per-key data in the ColumnarMapIndex itself.
 * This collector only tracks document count; no min/max/cardinality/dictionary is needed.
 */
public class ColumnarMapColumnPreIndexStatsCollector extends AbstractColumnStatisticsCollector {

  private boolean _sealed = false;

  public ColumnarMapColumnPreIndexStatsCollector(String column, StatsCollectorConfig statsCollectorConfig) {
    super(column, statsCollectorConfig);
    _sorted = false;
  }

  @Override
  public void collect(Object entry) {
    assert !_sealed;
    _totalNumberOfEntries++;
  }

  @Override
  public Object getMinValue() {
    return null;
  }

  @Override
  public Object getMaxValue() {
    return null;
  }

  @Override
  public Object getUniqueValuesSet() {
    return null;
  }

  @Override
  public int getCardinality() {
    return 0;
  }

  @Override
  public void seal() {
    _sealed = true;
  }
}
