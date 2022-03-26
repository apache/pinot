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

import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.query.planner.nodes.CalcNode;
import org.apache.pinot.query.planner.nodes.MailboxReceiveNode;
import org.apache.pinot.query.planner.nodes.MailboxSendNode;
import org.apache.pinot.query.planner.nodes.StageNode;
import org.apache.pinot.query.planner.nodes.TableScanNode;
import org.apache.pinot.query.runtime.plan.DistributedQueryPlan;


/**
 * {@code ServerRequestUtils} converts the {@link DistributedQueryPlan} into a {@link ServerQueryRequest}.
 *
 * <p>In order to reuse the current pinot {@link org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl}, a
 * conversion step is needed so that the V2 query plan can be converted into a compatible format to run V1 executor.
 */
public class ServerRequestUtils {

  private ServerRequestUtils() {
    // do not instantiate.
  }

  // TODO: This is a hack, make an actual ServerQueryRequest converter.
  public static ServerQueryRequest constructServerQueryRequest(DistributedQueryPlan distributedQueryPlan,
      Map<String, String> requestMetadataMap) {
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(Long.parseLong(requestMetadataMap.get("REQUEST_ID")));
    instanceRequest.setBrokerId("unknown");
    instanceRequest.setEnableTrace(false);
    instanceRequest.setSearchSegments(
        distributedQueryPlan.getMetadataMap().get(distributedQueryPlan.getStageId()).getServerInstanceToSegmentsMap()
            .get(distributedQueryPlan.getServerInstance()));
    instanceRequest.setQuery(constructBrokerRequest(distributedQueryPlan));
    return new ServerQueryRequest(instanceRequest, new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()),
        System.currentTimeMillis());
  }

  // TODO: this is a hack, create a broker request object should not be needed because we rewrite the entire
  // query into stages already.
  public static BrokerRequest constructBrokerRequest(DistributedQueryPlan distributedQueryPlan) {
    PinotQuery pinotQuery = constructPinotQuery(distributedQueryPlan);
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

  public static PinotQuery constructPinotQuery(DistributedQueryPlan distributedQueryPlan) {
    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.setExplain(false);
    walkStageTree(distributedQueryPlan.getStageRoot(), pinotQuery);
    return pinotQuery;
  }

  private static void walkStageTree(StageNode node, PinotQuery pinotQuery) {
    if (node instanceof CalcNode) {
      // TODO: add conversion for CalcNode, specifically filter/alias/...
    } else if (node instanceof TableScanNode) {
      TableScanNode tableScanNode = (TableScanNode) node;
      DataSource dataSource = new DataSource();
      dataSource.setTableName(tableScanNode.getTableName().get(0));
      pinotQuery.setDataSource(dataSource);
      pinotQuery.setSelectList(tableScanNode.getTableScanColumns().stream().map(RequestUtils::getIdentifierExpression)
          .collect(Collectors.toList()));
    } else if (node instanceof MailboxSendNode || node instanceof MailboxReceiveNode) {
      // ignore for now. continue to child.
    } else {
      throw new UnsupportedOperationException("Unsupported logical plan node: " + node);
    }
    for (StageNode child : node.getInputs()) {
      walkStageTree(child, pinotQuery);
    }
  }
}
