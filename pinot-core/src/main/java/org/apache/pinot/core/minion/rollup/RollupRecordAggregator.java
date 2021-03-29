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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.minion.segment.RecordAggregator;
import org.apache.pinot.core.segment.processing.collector.ValueAggregator;
import org.apache.pinot.core.segment.processing.collector.ValueAggregatorFactory;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Record aggregator implementation for roll-up segment converter.
 *
 * Given the list of rows with the same dimension values, the record aggregator aggregates rows into a single row
 * whose metric column values are aggregated based on the given aggregator functions.
 */
public class RollupRecordAggregator implements RecordAggregator {
  private static final String DEFAULT_VALUE_AGGREGATOR_TYPE = ValueAggregatorFactory.ValueAggregatorType.SUM.toString();

  private final Map<String, ValueAggregator> _valueAggregatorMap;
  private final Schema _schema;

  public RollupRecordAggregator(Schema schema, Map<String, String> aggregateTypes) {
    _schema = schema;
    _valueAggregatorMap = new HashMap<>();
    if (aggregateTypes == null) {
      aggregateTypes = Collections.emptyMap();
    }
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn() && fieldSpec.getFieldType() == FieldSpec.FieldType.METRIC) {
        String metricName = fieldSpec.getName();
        String aggregateType = aggregateTypes.getOrDefault(metricName, DEFAULT_VALUE_AGGREGATOR_TYPE);
        ValueAggregator valueAggregator = ValueAggregatorFactory.getValueAggregator(aggregateType, fieldSpec.getDataType());
        _valueAggregatorMap.put(metricName, valueAggregator);
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
        ValueAggregator aggregator = _valueAggregatorMap.get(metricName);
        Object aggregatedResult = aggregator.aggregate(resultRow.getValue(metricName), currentRow.getValue(metricName));
        resultRow.putValue(metricName, aggregatedResult);
      }
    }
    return resultRow;
  }
}
