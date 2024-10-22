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

package org.apache.pinot.common.metrics;

import java.util.List;
import org.testng.annotations.DataProvider;


public abstract class BrokerPrometheusMetricsTest extends PinotPrometheusMetricsTest {

  protected static final String EXPORTED_METRIC_PREFIX = "pinot_broker_";

  protected static final String EXPORTED_METRIC_PREFIX_EXCEPTIONS = "exceptions";

  protected static final List<BrokerMeter> GLOBAL_METERS_WITH_EXCEPTIONS_PREFIX =
      List.of(BrokerMeter.UNCAUGHT_GET_EXCEPTIONS, BrokerMeter.UNCAUGHT_POST_EXCEPTIONS,
          BrokerMeter.QUERY_REJECTED_EXCEPTIONS, BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS,
          BrokerMeter.RESOURCE_MISSING_EXCEPTIONS);

  protected static final List<BrokerMeter> METERS_ACCEPTING_RAW_TABLENAME =
      List.of(BrokerMeter.QUERIES, BrokerMeter.NO_SERVER_FOUND_EXCEPTIONS, BrokerMeter.DOCUMENTS_SCANNED,
          BrokerMeter.ENTRIES_SCANNED_IN_FILTER, BrokerMeter.BROKER_RESPONSES_WITH_UNAVAILABLE_SEGMENTS,
          BrokerMeter.BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED,
          BrokerMeter.BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS,
          BrokerMeter.BROKER_RESPONSES_WITH_NUM_GROUPS_LIMIT_REACHED, BrokerMeter.BROKER_RESPONSES_WITH_TIMEOUTS,
          BrokerMeter.ENTRIES_SCANNED_POST_FILTER, BrokerMeter.TOTAL_SERVER_RESPONSE_SIZE,
          BrokerMeter.QUERY_QUOTA_EXCEEDED);

  @Override
  protected PinotComponent getPinotComponent() {
    return PinotComponent.BROKER;
  }

  @DataProvider(name = "brokerTimers")
  public Object[] brokerTimers() {
    return BrokerTimer.values();
  }

  @DataProvider(name = "brokerMeters")
  public Object[] brokerMeters() {
    return BrokerMeter.values();
  }

  @DataProvider(name = "brokerGauges")
  public Object[] brokerGauges() {
    return BrokerGauge.values();
  }
}
