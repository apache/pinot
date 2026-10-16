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
package org.apache.pinot.query.runtime.queries;

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.spi.executor.ExecutorServicePlugin;
import org.apache.pinot.spi.executor.ExecutorServiceProvider;


/// [ExecutorServicePlugin] for tests that need to act after an op chain is scheduled but before it starts.
///
/// While the gate is closed, the tasks that the threads of these executors submit are parked instead of run, until
/// [#openGate()]. In a query runner, these threads run `QueryRunner#processQueryBlocking`, so the parked tasks are the
/// op chains that it schedules. The stage submissions come from other threads and run normally.
///
/// The gate is static, so it is shared by all the executors of this type in the JVM. Tests that close it must open it
/// again when they finish.
@AutoService(ExecutorServicePlugin.class)
public class GatedExecutorServicePlugin implements ExecutorServicePlugin {
  public static final String ID = "gatedForTest";

  private static final Set<Thread> EXECUTOR_THREADS = ConcurrentHashMap.newKeySet();
  private static final AtomicInteger THREAD_COUNT = new AtomicInteger();
  private static final List<ParkedTask> PARKED_TASKS = new ArrayList<>();
  private static boolean _gateClosed;

  @Override
  public String id() {
    return ID;
  }

  @Override
  public ExecutorServiceProvider provider() {
    return (conf, confPrefix, baseName) -> new GatedExecutorService(Executors.newCachedThreadPool(runnable -> {
      Thread thread = new Thread(runnable, baseName + "-gated-" + THREAD_COUNT.incrementAndGet());
      thread.setDaemon(true);
      EXECUTOR_THREADS.add(thread);
      return thread;
    }));
  }

  public static synchronized void closeGate() {
    _gateClosed = true;
  }

  /// Opens the gate and starts the parked tasks.
  public static void openGate() {
    List<ParkedTask> parkedTasks;
    synchronized (GatedExecutorServicePlugin.class) {
      _gateClosed = false;
      parkedTasks = new ArrayList<>(PARKED_TASKS);
      PARKED_TASKS.clear();
    }
    for (ParkedTask parkedTask : parkedTasks) {
      parkedTask._executorService.execute(parkedTask._task);
    }
  }

  public static synchronized int getNumParkedTasks() {
    return PARKED_TASKS.size();
  }

  private static class ParkedTask {
    final ExecutorService _executorService;
    final Runnable _task;

    ParkedTask(ExecutorService executorService, Runnable task) {
      _executorService = executorService;
      _task = task;
    }
  }

  private static class GatedExecutorService extends AbstractExecutorService {
    private final ExecutorService _delegate;

    GatedExecutorService(ExecutorService delegate) {
      _delegate = delegate;
    }

    @Override
    public void execute(Runnable task) {
      synchronized (GatedExecutorServicePlugin.class) {
        if (_gateClosed && EXECUTOR_THREADS.contains(Thread.currentThread())) {
          PARKED_TASKS.add(new ParkedTask(_delegate, task));
          return;
        }
      }
      _delegate.execute(task);
    }

    @Override
    public void shutdown() {
      _delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      return _delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
      return _delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return _delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
        throws InterruptedException {
      return _delegate.awaitTermination(timeout, unit);
    }
  }
}
