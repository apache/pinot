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

package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;


/**
 * {@code OpChainStats} tracks execution statistics for {@link OpChain}s.
 */
@NotThreadSafe
public class OpChainStats {

  // use memoized supplier so that the timing doesn't start until the
  // first time we get the timer
  private final Supplier<ThreadResourceUsageProvider> _exTimer =
      Suppliers.memoize(ThreadResourceUsageProvider::new)::get;

  // this is used to make sure that toString() doesn't have side
  // effects (accidentally starting the timer)
  private volatile boolean _exTimerStarted = false;

  private final Stopwatch _executeStopwatch = Stopwatch.createUnstarted();
  private final Stopwatch _queuedStopwatch = Stopwatch.createUnstarted();
  private final AtomicLong _queuedCount = new AtomicLong();

  private final String _id;
  private final ConcurrentHashMap<String, OperatorStats> _operatorStatsMap = new ConcurrentHashMap<>();

  public OpChainStats(String id) {
    _id = id;
  }

  public void executing() {
    startExecutionTimer();
    if (_queuedStopwatch.isRunning()) {
      _queuedStopwatch.stop();
    }
  }

  public void queued() {
    _queuedCount.incrementAndGet();
    if (!_queuedStopwatch.isRunning()) {
      _queuedStopwatch.start();
    }
    if (_executeStopwatch.isRunning()) {
      _executeStopwatch.stop();
    }
  }

  public ConcurrentHashMap<String, OperatorStats> getOperatorStatsMap() {
    return _operatorStatsMap;
  }

  public OperatorStats getOperatorStats(OpChainExecutionContext context, String operatorId) {
      return _operatorStatsMap.computeIfAbsent(operatorId, (id) -> {
        OperatorStats operatorStats = new OperatorStats(context);
        if (context.isTraceEnabled()) {
          operatorStats.recordSingleStat(DataTable.MetadataKey.OPERATOR_ID.getName(), operatorId);
        }
        return operatorStats;
      });
  }

  private void startExecutionTimer() {
    _exTimerStarted = true;
    _exTimer.get();
    if (!_executeStopwatch.isRunning()) {
      _executeStopwatch.start();
    }
  }

  public long getExecutionTime() {
    return _executeStopwatch.elapsed(TimeUnit.MILLISECONDS);
  }

  @Override
  public String toString() {
    return String.format("(%s) Queued Count: %s, Executing Time: %sms, Queued Time: %sms", _id, _queuedCount.get(),
        _exTimerStarted ? _executeStopwatch.elapsed(TimeUnit.MILLISECONDS) : 0,
        _queuedStopwatch.elapsed(TimeUnit.MILLISECONDS));
  }
}
