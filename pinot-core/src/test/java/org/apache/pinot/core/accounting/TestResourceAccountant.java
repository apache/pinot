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

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


class TestResourceAccountant extends PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant {
  TestResourceAccountant(Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries) {
    super(new PinotConfiguration(), false, true, true, new HashSet<>(), "test", InstanceType.SERVER);
    _threadEntriesMap.putAll(threadEntries);
  }

  static void getQueryThreadEntries(String queryId, CountDownLatch threadLatch,
      Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries) {
    TaskThread
        anchorThread = getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID, threadLatch, null);
    threadEntries.put(anchorThread._workerThread, anchorThread._threadEntry);
    anchorThread._threadEntry._currentThreadMemoryAllocationSampleBytes = 1000;

    CPUMemThreadLevelAccountingObjects.ThreadEntry anchorEntry = new CPUMemThreadLevelAccountingObjects.ThreadEntry();
    anchorEntry._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID,
            ThreadExecutionContext.TaskType.SSE, anchorThread._workerThread,
            CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME));
    anchorEntry._currentThreadMemoryAllocationSampleBytes = 1000;
    threadEntries.put(anchorThread._workerThread, anchorEntry);

    TaskThread taskThread2 = getTaskThread(queryId, 2, threadLatch, anchorThread._workerThread);
    threadEntries.put(taskThread2._workerThread, taskThread2._threadEntry);
    taskThread2._threadEntry._currentThreadMemoryAllocationSampleBytes = 2000;

    TaskThread taskThread3 = getTaskThread(queryId, 3, threadLatch, anchorThread._workerThread);
    threadEntries.put(taskThread3._workerThread, taskThread3._threadEntry);
    taskThread3._threadEntry._currentThreadMemoryAllocationSampleBytes = 2500;
  }

  private static TaskThread getTaskThread(String queryId, int taskId, CountDownLatch threadLatch, Thread anchorThread) {
    CPUMemThreadLevelAccountingObjects.ThreadEntry worker1 = new CPUMemThreadLevelAccountingObjects.ThreadEntry();
    worker1._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, taskId, ThreadExecutionContext.TaskType.SSE,
            anchorThread, CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME));
    Thread workerThread1 = new Thread(() -> {
      try {
        threadLatch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    workerThread1.start();
    return new TaskThread(worker1, workerThread1);
  }

  public TaskThread getTaskThread(String queryId, int taskId) {
    Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> workerEntry =
        _threadEntriesMap.entrySet().stream().filter(
            e -> e.getValue()._currentThreadTaskStatus.get().getTaskId() == 3 && Objects.equals(
                e.getValue()._currentThreadTaskStatus.get().getQueryId(), queryId)).collect(Collectors.toList()).get(0);
    return new TaskThread(workerEntry.getValue(), workerEntry.getKey());
  }

  public static class TaskThread {
    public final CPUMemThreadLevelAccountingObjects.ThreadEntry _threadEntry;
    public final Thread _workerThread;

    public TaskThread(CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry, Thread workerThread) {
      _threadEntry = threadEntry;
      _workerThread = workerThread;
    }
  }
}
