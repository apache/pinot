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
package org.apache.pinot.query.runtime.utils;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.ServerRequestPlanVisitor;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.metrics.PinotMetricUtils;


/**
 * {@code ServerRequestUtils} converts the {@link DistributedStagePlan} into a {@link ServerQueryRequest}.
 *
 * <p>In order to reuse the current pinot {@link org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl}, a
 * conversion step is needed so that the V2 query plan can be converted into a compatible format to run V1 executor.
 */
public class ServerRequestUtils {

  private ServerRequestUtils() {
    // do not instantiate.
  }

  public static ServerQueryRequest build(DistributedStagePlan stagePlan,
      Map<String, String> requestMetadataMap, TableConfig tableConfig, Schema schema,
      TimeBoundaryInfo timeBoundaryInfo, TableType tableType, List<String> segmentList) {
    PinotQuery pinotQuery = ServerRequestPlanVisitor.build(stagePlan, tableConfig, schema, timeBoundaryInfo, tableType);
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(Long.parseLong(requestMetadataMap.get("REQUEST_ID")));
    instanceRequest.setBrokerId("unknown");
    instanceRequest.setEnableTrace(false);
    instanceRequest.setSearchSegments(segmentList);
    instanceRequest.setQuery(toBrokerRequest(pinotQuery));
    return new ServerQueryRequest(instanceRequest, new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()),
        System.currentTimeMillis());
  }

  // TODO: this is a hack, create a broker request object should not be needed because we rewrite the entire
  // query into stages already.
  public static BrokerRequest toBrokerRequest(PinotQuery pinotQuery) {
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setPinotQuery(pinotQuery);
    // Set table name in broker request because it is used for access control, query routing etc.
    DataSource dataSource = pinotQuery.getDataSource();
    if (dataSource != null) {
      QuerySource querySource = new QuerySource();
      querySource.setTableName(dataSource.getTableName());
      brokerRequest.setQuerySource(querySource);
    }
    return brokerRequest;
  }
}
