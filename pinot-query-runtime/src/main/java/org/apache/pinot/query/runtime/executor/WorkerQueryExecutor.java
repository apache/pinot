package org.apache.pinot.query.runtime.executor;

import java.util.concurrent.ExecutorService;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.query.dispatch.WorkerQueryRequest;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * WorkerQueryExecutor is the v2 of the {@link org.apache.pinot.core.query.executor.QueryExecutor} API.
 *
 * It provides not only execution interface for {@link org.apache.pinot.core.query.request.ServerQueryRequest} but
 * also a more general {@link WorkerQueryRequest}.
 */
public class WorkerQueryExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerQueryExecutor.class);
  private PinotConfiguration _config;
  private ServerMetrics _serverMetrics;

  public void init(PinotConfiguration config, ServerMetrics serverMetrics) {
    _config = config;
    _serverMetrics = serverMetrics;
  }

  public synchronized void start() {
    LOGGER.info("Worker query executor started");
  }

  public synchronized void shutDown() {
    LOGGER.info("Worker query executor shut down");
  }

  public void processQuery(WorkerQueryRequest queryRequest, ExecutorService executorService) {

  }
}
