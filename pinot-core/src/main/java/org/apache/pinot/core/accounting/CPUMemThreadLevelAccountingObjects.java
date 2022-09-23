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

import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.spi.accounting.ExecutionContext;


/**
 * Entries for thread level stats and task info collection used on server/broker
 */
public class CPUMemThreadLevelAccountingObjects {

  public static class StatsDigest {

    final long[] _statsEntries;
    final long[] _lastStatSample;
    final HashMap<String, Long> _finishedTaskStatAggregator;

    StatsDigest(int numThreads) {
      _statsEntries = new long[numThreads];
      _lastStatSample = new long[numThreads];
      _finishedTaskStatAggregator = new HashMap<>();
    }
  }

  /**
   * Entry to track the task execution status of a worker/runner given thread
   */
  public static class TaskEntryHolder {
    AtomicReference<TaskEntry> _threadTaskStatus = new AtomicReference<>(null);

    /**
     * set the thread tracking info to null
     */
    public void setToIdle() {
      _threadTaskStatus.set(null);
    }

    /**
     *
     * @return the current query id on the thread, {@code null} if idle
     */
    @Nullable
    public TaskEntry getThreadTaskStatus() {
      return _threadTaskStatus.get();
    }

    public TaskEntryHolder setThreadTaskStatus(@Nonnull String queryId, int taskId, @Nonnull Thread thread) {
      _threadTaskStatus.set(new TaskEntry(queryId, taskId, thread));
      return this;
    }
  }

  public static class TaskEntry implements ExecutionContext {
    private final String _queryId;
    private final int _taskId;
    private final Thread _rootThread;

    public TaskEntry(String queryId, int taskId, Thread rootThread) {
      _queryId = queryId;
      _taskId = taskId;
      _rootThread = rootThread;
    }

    public static boolean isSameTask(TaskEntry currentTaskStatus, TaskEntry lastQueryTask) {
      if (currentTaskStatus == null) {
        return lastQueryTask == null;
      } else if (lastQueryTask == null) {
        return false;
      } else {
        return Objects.equals(currentTaskStatus.getQueryId(), lastQueryTask.getQueryId())
            || currentTaskStatus.getTaskId() == lastQueryTask.getTaskId();
      }
    }

    public String getQueryId() {
      return _queryId;
    }

    public int getTaskId() {
      return _taskId;
    }

    public Thread getRootThread() {
      return _rootThread;
    }

    @Override
    public String toString() {
      return "TaskEntry{" + "_queryId='" + _queryId + '\'' + ", _taskId=" + _taskId + ", _rootThread=" + _rootThread
          + '}';
    }
  }
}
