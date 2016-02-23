/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.transport.scattergather;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * A class to record statistics for scatter-gather requests, on a per-server basis.
 * NOTE: Assumes there will not be more than one thread updating stats for a server.
 */
public class ScatterGatherStats {
  private ConcurrentMap<String, PerServerStats> _perServerStatsMap;

  public void setSendStartTimeMillis(String server, long millis) {
    PerServerStats perServerStats = _perServerStatsMap.get(server);
    if (perServerStats != null) {
      perServerStats.setSendStartTimeMillis(millis);
    }
  }

  public void setConnStartTimeMillis(String server, long millis) {
    PerServerStats perServerStats = _perServerStatsMap.get(server);
    if (perServerStats != null) {
      perServerStats.setConnStartDelayMillis(millis);
    }
  }

  public void setSendCompletionTimeMillis(String server, long millis) {
    PerServerStats perServerStats = _perServerStatsMap.get(server);
    if (perServerStats != null) {
      perServerStats.setSendCompletionTimeMillis(millis);
    }
  }

  public void setResponseTimeMillis(Map<String, Long> responseTimesMap) {
    for (Map.Entry<String, Long> entry : responseTimesMap.entrySet()) {
      PerServerStats perServerStats = _perServerStatsMap.get(entry.getKey());
      if (perServerStats != null) {
        perServerStats.setResponseTimeMillis(entry.getValue());
      }
    }
  }

  public ScatterGatherStats() {
    _perServerStatsMap = new ConcurrentHashMap<>(10);
  }

  public void initServer(String server) {
    PerServerStats perServerStats = new PerServerStats();
    _perServerStatsMap.put(server, perServerStats);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<String, PerServerStats> entry : _perServerStatsMap.entrySet()) {
      builder.append(entry.getKey().split("\\.")[0]).append("=").append(entry.getValue()).append(":");
    }
    return builder.toString();
  }

  // Merge statistics from 'other' to this one (for new servers)
  public void merge(ScatterGatherStats other) {
    Map<String, PerServerStats> perServerStatsMap = other.getPerServerStatMap();
    for (Map.Entry<String, PerServerStats> entry : perServerStatsMap.entrySet()) {
      _perServerStatsMap.putIfAbsent(entry.getKey(), entry.getValue());
    }
  }

  private Map<String, PerServerStats> getPerServerStatMap() {
    return _perServerStatsMap;
  }

  private class PerServerStats {
    // All times are relative to 0 when the request starts.
    private long _connStartDelayMillis;
    private long _sendStartTimeMillis;
    private long _sendCompletionTimeMillis;
    private long _responseTimeMillis;

    private PerServerStats() {
    }

    private long getConnStartDelayMillis() {
      return _connStartDelayMillis;
    }

    private void setConnStartDelayMillis(long connStartDelayMillis) {
      _connStartDelayMillis = connStartDelayMillis;
    }

    private long getSendStartTimeMillis() {
      return _sendStartTimeMillis;
    }

    private void setSendStartTimeMillis(long sendStartTimeMillis) {
      _sendStartTimeMillis = sendStartTimeMillis;
    }

    private long getSendCompletionTimeMillis() {
      return _sendCompletionTimeMillis;
    }

    private void setSendCompletionTimeMillis(long sendCompletionTimeMillis) {
      _sendCompletionTimeMillis = sendCompletionTimeMillis;
    }

    private long getResponseTimeMillis() {
      return _responseTimeMillis;
    }

    private void setResponseTimeMillis(long responseTimeMillis) {
      _responseTimeMillis = responseTimeMillis;
    }

    @Override
    public String toString() {
      return String.format("%d,%d,%d,%d", getConnStartDelayMillis(), getSendStartTimeMillis(), getSendCompletionTimeMillis(), getResponseTimeMillis());
    }
  }
}
