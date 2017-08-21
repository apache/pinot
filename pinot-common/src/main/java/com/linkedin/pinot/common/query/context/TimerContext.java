/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.common.query.context;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.metrics.ServerQueryPhase;
import com.linkedin.pinot.common.request.BrokerRequest;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

public class TimerContext {

  private final ServerMetrics serverMetrics;
  private final Map<ServerQueryPhase, Timer> phaseTimers = new HashMap<>();
  private final BrokerRequest brokerRequest;
  private long queryArrivalTimeNs;

  public class Timer implements AutoCloseable {
    private final ServerQueryPhase queryPhase;
    long startTimeNs;
    long endTimeNs;
    boolean isRunning;

    Timer(ServerQueryPhase phase) {
      this.queryPhase = phase;
    }

    void start() {
      startTimeNs = System.nanoTime();
      isRunning = true;
    }

    void setStartTimeNs(long startTimeNs) {
      this.startTimeNs = startTimeNs;
      isRunning = true;
    }

    public void stopAndRecord() {
      if (isRunning) {
        endTimeNs = System.nanoTime();

        recordPhaseTime(this);
        isRunning = false;
      }
    }

    public long getDurationNs() {
      return endTimeNs - startTimeNs;
    }

    public long getDurationMs() {
      return TimeUnit.MILLISECONDS.convert(endTimeNs - startTimeNs, TimeUnit.NANOSECONDS);
    }
    @Override
    public void close()
        throws Exception {
      stopAndRecord();
    }
  }

  public TimerContext(BrokerRequest brokerRequest, ServerMetrics serverMetrics) {
    this.brokerRequest = brokerRequest;
    this.serverMetrics = serverMetrics;
  }

  public void setQueryArrivalTimeNs(long queryStartTimeNs) {
    this.queryArrivalTimeNs = queryStartTimeNs;
  }

  public long getQueryArrivalTimeNs() {
    return queryArrivalTimeNs;
  }

  public long getQueryArrivalTimeMs() {
    return TimeUnit.MILLISECONDS.convert(queryArrivalTimeNs, TimeUnit.NANOSECONDS);
  }

  /**
   * Creates and starts a new timer for query phase.
   * Calling this again for same phase will overwrite existing timing information
   * @param queryPhase query phase that is being timed
   * @return
   */
  public Timer startNewPhaseTimer(ServerQueryPhase queryPhase) {
    Timer phaseTimer = new Timer(queryPhase);
    phaseTimers.put(queryPhase, phaseTimer);
    phaseTimer.start();
    return phaseTimer;
  }

  public Timer startNewPhaseTimerAtNs(ServerQueryPhase queryPhase, long startTimeNs) {
    Timer phaseTimer = startNewPhaseTimer(queryPhase);
    phaseTimer.setStartTimeNs(startTimeNs);
    return phaseTimer;
  }

  public @Nullable Timer getPhaseTimer(ServerQueryPhase queryPhase) {
    return phaseTimers.get(queryPhase);
  }

  public long getPhaseDurationNs(ServerQueryPhase queryPhase) {
    Timer timer = phaseTimers.get(queryPhase);
    if (timer == null) {
      return -1;
    }
    return timer.getDurationNs();
  }

  public long getPhaseDurationMs(ServerQueryPhase queryPhase) {
    return TimeUnit.MILLISECONDS.convert(getPhaseDurationNs(queryPhase), TimeUnit.NANOSECONDS);
  }

  void recordPhaseTime(Timer phaseTimer) {
    serverMetrics.addPhaseTiming(brokerRequest, phaseTimer.queryPhase, phaseTimer.getDurationNs());
  }

  // for logging
  @Override
  public String toString() {
    return String.format("%d,%d,%d,%d,%d,%d,%d,%d (in ns)", queryArrivalTimeNs,
        getPhaseDurationNs(ServerQueryPhase.REQUEST_DESERIALIZATION),
        getPhaseDurationNs(ServerQueryPhase.SCHEDULER_WAIT),
        getPhaseDurationNs(ServerQueryPhase.BUILD_QUERY_PLAN),
        getPhaseDurationNs(ServerQueryPhase.QUERY_PLAN_EXECUTION),
        getPhaseDurationNs(ServerQueryPhase.QUERY_PROCESSING),
        getPhaseDurationNs(ServerQueryPhase.RESPONSE_SERIALIZATION),
        getPhaseDurationNs(ServerQueryPhase.TOTAL_QUERY_TIME));
  }
}
