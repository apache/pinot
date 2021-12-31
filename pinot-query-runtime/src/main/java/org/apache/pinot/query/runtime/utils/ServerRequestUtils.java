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
import org.apache.pinot.query.dispatch.WorkerQueryRequest;
import org.apache.pinot.query.planner.nodes.CalcNode;
import org.apache.pinot.query.planner.nodes.MailboxReceiveNode;
import org.apache.pinot.query.planner.nodes.MailboxSendNode;
import org.apache.pinot.query.planner.nodes.StageNode;
import org.apache.pinot.query.planner.nodes.TableScanNode;


public class ServerRequestUtils {

  private ServerRequestUtils() {
    // do not instantiate.
  }

  // TODO: This is a hack, make an actual ServerQueryRequest converter.
  public static ServerQueryRequest constructServerQueryRequest(WorkerQueryRequest workerQueryRequest,
      Map<String, String> requestMetadataMap) {
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(Long.parseLong(requestMetadataMap.get("RequestId")));
    instanceRequest.setBrokerId("unknown");
    instanceRequest.setEnableTrace(false);
    instanceRequest.setSearchSegments(workerQueryRequest.getMetadataMap().get(workerQueryRequest.getStageId())
        .getServerInstanceToSegmentsMap().get(workerQueryRequest.getServerInstance()));
    instanceRequest.setQuery(constructBrokerRequest(workerQueryRequest));
    return new ServerQueryRequest(instanceRequest, new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()),
        System.currentTimeMillis());
  }

  // TODO: this is a hack, create a broker request object should not be needed because we rewrite the entire
  // query into stages already.
  public static BrokerRequest constructBrokerRequest(WorkerQueryRequest workerQueryRequest) {
    PinotQuery pinotQuery = constructPinotQuery(workerQueryRequest);
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

  public static PinotQuery constructPinotQuery(WorkerQueryRequest workerQueryRequest) {
    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.setExplain(false);
    walkStageTree(workerQueryRequest.getStageRoot(), pinotQuery);
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
      pinotQuery.setSelectList(tableScanNode.getTableScanColumns().stream()
          .map(RequestUtils::getIdentifierExpression)
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
