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
package org.apache.pinot.core.accounting;

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Entries for thread level stats and task info collection used on server/broker
 */
public class CPUMemThreadLevelAccountingObjects {

  /**
   * Entry to track the task execution status and usage stats of a Thread
   * (including but not limited to server worker thread, runner thread, broker jetty thread, or broker netty thread)
   */
  public static class ThreadEntry {
    // current query_id, task_id of the thread; this field is accessed by the thread itself and the accountant
    AtomicReference<TaskEntry> _currentThreadTaskStatus = new AtomicReference<>();
    // current sample of thread memory usage/cputime ; this field is accessed by the thread itself and the accountant
    volatile long _currentThreadCPUTimeSampleMS = 0;
    volatile long _currentThreadMemoryAllocationSampleBytes = 0;

    // previous query_id, task_id of the thread, this field should only be accessed by the accountant
    TaskEntry _previousThreadTaskStatus = null;
    // previous cpu time and memory allocation of the thread
    // these fields should only be accessed by the accountant
    long _previousThreadCPUTimeSampleMS = 0;
    long _previousThreadMemoryAllocationSampleBytes = 0;

    // error message store per runner/worker thread,
    // will put preemption reasons in this for the killed thread to pickup
    AtomicReference<Exception> _errorStatus = new AtomicReference<>();

    @Override
    public String toString() {
      TaskEntry taskEntry = _currentThreadTaskStatus.get();
      return "ThreadEntry{"
          + "_currentThreadTaskStatus=" + (taskEntry == null ? "idle" : taskEntry.toString())
          + ", _errorStatus=" + _errorStatus
          + '}';
    }

    /**
     * set the thread tracking info to null and usage samples to zero
     */
    public void setToIdle() {
      // clear task info
      _currentThreadTaskStatus.set(null);
      // clear CPU time
      _currentThreadCPUTimeSampleMS = 0;
      // clear memory usage
      _currentThreadMemoryAllocationSampleBytes = 0;
    }

    /**
     *
     * @return the current query id on the thread, {@code null} if idle
     */
    @Nullable
    public TaskEntry getCurrentThreadTaskStatus() {
      return _currentThreadTaskStatus.get();
    }

    public void setThreadTaskStatus(@Nonnull String queryId, int taskId, @Nonnull Thread anchorThread) {
      _currentThreadTaskStatus.set(new TaskEntry(queryId, taskId, anchorThread));
    }
  }

  /**
   * Class to track the execution status of a thread. query_id is an instance level unique query_id,
   * taskId is the worker thread id when we have a runner-worker thread model
   * anchor thread refers to the runner in runner-worker thread model
   */
  public static class TaskEntry implements ThreadExecutionContext {
    private final String _queryId;
    private final int _taskId;
    private final Thread _anchorThread;

    public boolean isAnchorThread() {
      return _taskId == CommonConstants.Accounting.ANCHOR_TASK_ID;
    }

    public TaskEntry(String queryId, int taskId, Thread anchorThread) {
      _queryId = queryId;
      _taskId = taskId;
      _anchorThread = anchorThread;
    }

    public String getQueryId() {
      return _queryId;
    }

    public int getTaskId() {
      return _taskId;
    }

    public Thread getAnchorThread() {
      return _anchorThread;
    }

    @Override
    public String toString() {
      return "TaskEntry{" + "_queryId='" + _queryId + '\'' + ", _taskId=" + _taskId + ", _rootThread=" + _anchorThread
          + '}';
    }
  }
}
