/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.query.scheduler;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.ServerQueryRequest;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.request.QuerySource;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

  public static ServerQueryRequest createServerQueryRequest(String table, ServerMetrics metrics) {
    InstanceRequest request = new InstanceRequest();
    request.setBrokerId("broker");
    request.setEnableTrace(false);
    request.setRequestId(1);
    request.setSearchSegments(Arrays.asList("segment1", "segment2"));
    BrokerRequest br = new BrokerRequest();
    QuerySource qs = new QuerySource();
    qs.setTableName(table);
    br.setQuerySource(qs);
    request.setQuery(br);
    ServerQueryRequest qr = new ServerQueryRequest(request, metrics);
    qr.getTimerContext().setQueryArrivalTimeNs(
        TimeUnit.NANOSECONDS.convert(
            System.currentTimeMillis(), TimeUnit.MILLISECONDS));
    return qr;
  }

  public static SchedulerQueryContext createQueryRequest(String table, ServerMetrics metrics) {
    return new SchedulerQueryContext(createServerQueryRequest(table, metrics));
  }

}
