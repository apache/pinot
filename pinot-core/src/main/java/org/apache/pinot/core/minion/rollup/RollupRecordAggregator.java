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
package org.apache.pinot.core.minion.rollup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.core.segment.processing.collector.ValueAggregator;
import org.apache.pinot.core.segment.processing.collector.ValueAggregatorFactory;
import org.apache.pinot.core.minion.segment.RecordAggregator;


/**
 * Record aggregator implementation for roll-up segment converter.
 *
 * Given the list of rows with the same dimension values, the record aggregator aggregates rows into a single row
 * whose metric column values are aggregated based on the given aggregator functions.
 */
public class RollupRecordAggregator implements RecordAggregator {
  private final static ValueAggregator DEFAULT_AGGREGATOR_FUNCTION =
      ValueAggregatorFactory.getValueAggregator(ValueAggregatorFactory.ValueAggregatorType.SUM.name());

  private Map<String, ValueAggregator> _valueAggregatorMap;
  private Schema _schema;

  public RollupRecordAggregator(Schema schema, Map<String, String> aggregateTypes) {
    _schema = schema;
    _valueAggregatorMap = new HashMap<>();
    if (aggregateTypes != null) {
      for (Map.Entry<String, String> entry : aggregateTypes.entrySet()) {
        _valueAggregatorMap.put(entry.getKey(), ValueAggregatorFactory.getValueAggregator(entry.getValue()));
      }
    }
  }

  @Override
  public GenericRow aggregateRecords(List<GenericRow> rows) {
    GenericRow resultRow = rows.get(0);
    for (int i = 1; i < rows.size(); i++) {
      GenericRow currentRow = rows.get(i);
      for (MetricFieldSpec metric : _schema.getMetricFieldSpecs()) {
        String metricName = metric.getName();
        ValueAggregator aggregator = (_valueAggregatorMap == null) ? DEFAULT_AGGREGATOR_FUNCTION
            : _valueAggregatorMap.getOrDefault(metricName, DEFAULT_AGGREGATOR_FUNCTION);
        Object aggregatedResult =
            aggregator.aggregate(resultRow.getValue(metricName), currentRow.getValue(metricName), metric);
        resultRow.putField(metricName, aggregatedResult);
      }
    }
    return resultRow;
  }
}
