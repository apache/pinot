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
package org.apache.pinot.plugin.stream.kafka20.metrics;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.pinot.spi.metrics.StreamConsumerGauge;
import org.apache.pinot.spi.metrics.StreamConsumerMetrics;


public class KafkaConsumerMetrics {
  private final StreamConsumerMetrics _streamConsumerMetrics;
  private final String _clientId;
  private final String _topicName;
  private final int _partitionId;

  private final Map<String, String> _metricAttributes;

  public KafkaConsumerMetrics(String clientId, String topicName, int partitionId,
      StreamConsumerMetrics streamConsumerMetrics) {
    _streamConsumerMetrics = streamConsumerMetrics;
    _clientId = clientId;
    _topicName = topicName;
    _partitionId = partitionId;
    _metricAttributes = new HashMap<>();
    _metricAttributes.put("client-id", _clientId);
    _metricAttributes.put("topic", _topicName);
    _metricAttributes.put("partition", String.valueOf(_partitionId));
  }

  public void updateLag(Map<MetricName, ? extends Metric> metricsMap) {
    final MetricName name = new MetricName("records-lag", "consumer-fetch-manager-metrics",
        "ignore", _metricAttributes);
    // TODO: this expects tablename . but we are using topic name

    long lag = (long) ((double) metricsMap.get(name).metricValue());
//    System.out.println("Lag for " + _topicName + "-" + _partitionId + " = " + lag);
    _streamConsumerMetrics.setValueOfPartitionGauge(_topicName, _partitionId,
        StreamConsumerGauge.PARITTION_RECORDS_LAG, lag);
  }

  public void updateReceivedRecords(long count) {
    _streamConsumerMetrics.setValueOfPartitionGauge(_topicName, _partitionId,
        StreamConsumerGauge.RECEIVED_RECORDS, count);
  }
}
