package org.apache.pinot.spi.accounting;

public interface QueryCancelManager {
  /**
   * Cancels the query with the given queryId.
   *
   * @param queryId The ID of the query to cancel.
   */
  void cancelQuery(String queryId, ThreadExecutionContext.TaskType taskType);
}
