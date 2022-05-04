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
package org.apache.pinot.core.query.scheduler;

import java.util.Arrays;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;


public class TestHelper {
  private TestHelper() {
  }

  public static ServerQueryRequest createServerQueryRequest(String table, ServerMetrics metrics,
      long queryArrivalTimeMs) {
    InstanceRequest request = new InstanceRequest();
    request.setBrokerId("broker");
    request.setEnableTrace(false);
    request.setRequestId(1);
    request.setSearchSegments(Arrays.asList("segment1", "segment2"));
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM \"" + table + "\"");
    request.setQuery(brokerRequest);
    return new ServerQueryRequest(request, metrics, queryArrivalTimeMs);
  }

  public static ServerQueryRequest createServerQueryRequest(String table, ServerMetrics metrics) {
    return createServerQueryRequest(table, metrics, System.currentTimeMillis());
  }

  public static SchedulerQueryContext createQueryRequest(String table, ServerMetrics metrics, long queryArrivalTimeMs) {
    return new SchedulerQueryContext(createServerQueryRequest(table, metrics, queryArrivalTimeMs));
  }

  public static SchedulerQueryContext createQueryRequest(String table, ServerMetrics metrics) {
    return createQueryRequest(table, metrics, System.currentTimeMillis());
  }
}
