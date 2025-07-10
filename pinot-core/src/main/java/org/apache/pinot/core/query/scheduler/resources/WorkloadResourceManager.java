package org.apache.pinot.core.query.scheduler.resources;

import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.SchedulerGroupAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.env.PinotConfiguration;

public class WorkloadResourceManager extends ResourceManager {
  private final ResourceLimitPolicy _resourcePolicy;

  /**
   * @param config configuration for initializing resource manager
   */
  public WorkloadResourceManager(PinotConfiguration config, ThreadResourceUsageAccountant resourceUsageAccountant) {
    super(config, resourceUsageAccountant);
    _resourcePolicy = new ResourceLimitPolicy(config, _numQueryWorkerThreads);
  }

  @Override
  public QueryExecutorService getExecutorService(ServerQueryRequest query, SchedulerGroupAccountant accountant) {
    return new QueryExecutorService() {
      @Override
      public void execute(Runnable command) {
        _queryWorkers.submit(command);
      }
    };
  }

  @Override
  public int getTableThreadsHardLimit() {
    return _resourcePolicy.getTableThreadsHardLimit();
  }

  @Override
  public int getTableThreadsSoftLimit() {
    return _resourcePolicy.getTableThreadsSoftLimit();
  }
}
