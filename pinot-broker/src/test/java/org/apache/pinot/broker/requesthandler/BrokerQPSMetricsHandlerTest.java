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
package org.apache.pinot.broker.requesthandler;

import com.yammer.metrics.core.MetricsRegistry;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests the {@link BrokerQPSMetricsHandler} functionality
 */
public class BrokerQPSMetricsHandlerTest {

  /**
   * Tests the schedules in BrokerQPSMetricsHandler are emitting metrics
   */
    @Test
    public void testBrokerMetricsHandlerSchedules() {
      String table1 = "table1";
      String table2 = "table2";
      String table3 = "table3";
      BrokerMetrics brokerMetrics = new BrokerMetrics(new MetricsRegistry());

      BrokerQPSMetricsHandler _brokerMetricsHandler = new BrokerQPSMetricsHandler(brokerMetrics);
      _brokerMetricsHandler.start();

      // schedule to report query counts runs every 10s, with initial delay of 10s
      long nowPlus20Seconds = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(20, TimeUnit.SECONDS);
      while (System.currentTimeMillis() < nowPlus20Seconds) {
        _brokerMetricsHandler.incrementQueryCount(table1);
        _brokerMetricsHandler.incrementQueryCount(table2);
        _brokerMetricsHandler.incrementQueryCount(table3);
      }
      Assert.assertTrue(brokerMetrics.getValueOfTableGauge(table1, BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE) == 0);
      Assert.assertTrue(brokerMetrics.getValueOfTableGauge(table2, BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE) == 0);
      Assert.assertTrue(brokerMetrics.getValueOfTableGauge(table3, BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE) == 0);

      // schedule to emit metric runs every 60s with initial delay of 60s
      long nowPlus1Minute = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
      while (System.currentTimeMillis() < nowPlus1Minute) {
        _brokerMetricsHandler.incrementQueryCount(table1);
        _brokerMetricsHandler.incrementQueryCount(table2);
        _brokerMetricsHandler.incrementQueryCount(table3);
      }
      Assert.assertTrue(brokerMetrics.getValueOfTableGauge(table1, BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE) > 0);
      Assert.assertTrue(brokerMetrics.getValueOfTableGauge(table2, BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE) > 0);
      Assert.assertTrue(brokerMetrics.getValueOfTableGauge(table3, BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE) > 0);

      _brokerMetricsHandler.stop();
    }

  /**
   * Tests various scenarios of BrokerQPSMetricsHandler and gauge value
   */
  @Test
    public void testSettingGaugeValues() {
      String table1 = "table1";
      BrokerMetrics brokerMetrics = new BrokerMetrics(new MetricsRegistry());

      BrokerQPSMetricsHandler _brokerMetricsHandler = new BrokerQPSMetricsHandler(brokerMetrics);

      // Initially gauge will be 0
      Assert.assertEquals(brokerMetrics.getValueOfTableGauge(table1, BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE), 0);

      // no queries received, but gauge set by 1m schedule
      _brokerMetricsHandler.setMaxQueriesPer10SecGauge();
      Assert.assertEquals(brokerMetrics.getValueOfTableGauge(table1, BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE), 0);

      // received queries, but not reported before 1m schedule
      for (int i = 0; i < 10; i ++) {
        _brokerMetricsHandler.incrementQueryCount(table1);
      }
      _brokerMetricsHandler.setMaxQueriesPer10SecGauge();
      Assert.assertEquals(brokerMetrics.getValueOfTableGauge(table1, BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE), 0);

      // queries reported by 10s schedule, but 1m gauge schedule yet to run
      _brokerMetricsHandler.reportQueryCount();
      Assert.assertEquals(brokerMetrics.getValueOfTableGauge(table1, BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE), 0);

      // 1m gauge schedule run and gauge set, value will finally increase
      _brokerMetricsHandler.setMaxQueriesPer10SecGauge();
      Assert.assertEquals(brokerMetrics.getValueOfTableGauge(table1, BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE), 10);

      // report that no queries received again, set gauge. gauge will retain old value until unset
      _brokerMetricsHandler.reportQueryCount();
      Assert.assertEquals(brokerMetrics.getValueOfTableGauge(table1, BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE), 10);
      _brokerMetricsHandler.setMaxQueriesPer10SecGauge();
      Assert.assertEquals(brokerMetrics.getValueOfTableGauge(table1, BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE), 0);

      // received queries, and reported
      for (int i = 0; i < 8; i ++) {
        _brokerMetricsHandler.incrementQueryCount(table1);
      }
      _brokerMetricsHandler.reportQueryCount();
      // received queries again and reported
      for (int i = 0; i < 4; i ++) {
        _brokerMetricsHandler.incrementQueryCount(table1);
      }
      _brokerMetricsHandler.reportQueryCount();
      // set gauge, max value should be reported
      _brokerMetricsHandler.setMaxQueriesPer10SecGauge();
      Assert.assertEquals(brokerMetrics.getValueOfTableGauge(table1, BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE), 8);

      // gauge reset
      _brokerMetricsHandler.setMaxQueriesPer10SecGauge();
      Assert.assertEquals(brokerMetrics.getValueOfTableGauge(table1, BrokerGauge.MAX_QUERIES_PER_10_SEC_IN_MINUTE), 0);

    }

}

