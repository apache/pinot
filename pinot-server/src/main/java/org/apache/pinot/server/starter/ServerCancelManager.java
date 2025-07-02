package org.apache.pinot.server.starter;

import org.apache.pinot.core.transport.InstanceRequestHandler;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.spi.accounting.QueryCancelManager;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;


public class ServerCancelManager implements QueryCancelManager {
  private final InstanceRequestHandler _sseCancelHandler;
  private final QueryRunner _mseCancelHandler;

  public ServerCancelManager(InstanceRequestHandler sseCancelHandler, QueryRunner mseCancelHandler) {
    _sseCancelHandler = sseCancelHandler;
    _mseCancelHandler = mseCancelHandler;
  }

  @Override
  public void cancelQuery(String queryId, ThreadExecutionContext.TaskType taskType) {
    if (taskType == ThreadExecutionContext.TaskType.SSE) {
      _sseCancelHandler.cancelQuery(queryId);
    } else if (taskType == ThreadExecutionContext.TaskType.MSE) {
      _mseCancelHandler.cancel(Long.parseLong(queryId));
    } else {
      throw new IllegalArgumentException("Unsupported task type: " + taskType);
    }
  }
}
