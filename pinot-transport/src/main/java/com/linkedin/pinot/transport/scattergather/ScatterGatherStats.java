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
package com.linkedin.pinot.transport.scattergather;

import java.util.HashMap;
import java.util.Map;
import com.linkedin.pinot.common.response.ServerInstance;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * A class to record statistics for scatter-gather requests, on a per-server basis.
 */
@NotThreadSafe
public class ScatterGatherStats {
  public static final String OFFLINE_TABLE_SUFFIX = "_O";
  public static final String REALTIME_TABLE_SUFFIX = "_R";

  private Map<String, PerServerStats> _perServerStatsMap;

  public ScatterGatherStats() {
    _perServerStatsMap = new HashMap<>();
  }

  public void initServer(String server) {
    _perServerStatsMap.put(server, new PerServerStats());
  }

  public void setSendStartTimeMillis(String server, long millis) {
    _perServerStatsMap.get(server).setSendStartTimeMillis(millis);
  }

  public void setConnStartTimeMillis(String server, long millis) {
    _perServerStatsMap.get(server).setConnStartDelayMillis(millis);
  }

  public void setSendCompletionTimeMillis(String server, long millis) {
    _perServerStatsMap.get(server).setSendCompletionTimeMillis(millis);
  }

  public void setResponseTimeMillis(Map<ServerInstance, Long> responseTimeMap, boolean isOfflineTable) {
    for (Map.Entry<ServerInstance, Long> entry : responseTimeMap.entrySet()) {
      String shortServerName = entry.getKey().getShortHostName();
      if (isOfflineTable) {
        shortServerName += OFFLINE_TABLE_SUFFIX;
      } else {
        shortServerName += REALTIME_TABLE_SUFFIX;
      }
      _perServerStatsMap.get(shortServerName).setResponseTimeMillis(entry.getValue());
    }
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    boolean isFirstEntry = true;
    for (Map.Entry<String, PerServerStats> entry : _perServerStatsMap.entrySet()) {
      if (isFirstEntry) {
        isFirstEntry = false;
      } else {
        stringBuilder.append(';');
      }
      stringBuilder.append(entry.getKey()).append('=').append(entry.getValue());
    }
    return stringBuilder.toString();
  }

  private class PerServerStats {
    private long _connStartDelayMillis;
    private long _sendStartTimeMillis;
    private long _sendCompletionTimeMillis;
    private long _responseTimeMillis;

    private void setConnStartDelayMillis(long connStartDelayMillis) {
      _connStartDelayMillis = connStartDelayMillis;
    }

    private void setSendStartTimeMillis(long sendStartTimeMillis) {
      _sendStartTimeMillis = sendStartTimeMillis;
    }

    private void setSendCompletionTimeMillis(long sendCompletionTimeMillis) {
      _sendCompletionTimeMillis = sendCompletionTimeMillis;
    }

    private void setResponseTimeMillis(long responseTimeMillis) {
      _responseTimeMillis = responseTimeMillis;
    }

    @Override
    public String toString() {
      return String.format("%d,%d,%d,%d", _connStartDelayMillis, _sendStartTimeMillis, _sendCompletionTimeMillis,
          _responseTimeMillis);
    }
  }
}
