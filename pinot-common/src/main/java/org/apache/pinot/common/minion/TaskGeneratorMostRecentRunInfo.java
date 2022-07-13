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
package org.apache.pinot.common.minion;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import org.apache.helix.ZNRecord;


/**
 * a task generator running history which keeps the most recent several success run timestamp and the most recent
 * several error run
 * message.
 */
public class TaskGeneratorMostRecentRunInfo extends BaseTaskGeneratorInfo {
  @VisibleForTesting
  static final int MAX_NUM_OF_HISTORY_TO_KEEP = 5;
  @VisibleForTesting
  static final String MOST_RECENT_SUCCESS_RUN_TS = "mostRecentSuccessRunTs";
  @VisibleForTesting
  static final String MOST_RECENT_ERROR_RUN_MESSAGE = "mostRecentErrorRunMessage";

  private final String _taskType;
  private final String _tableNameWithType;
  // the timestamp to error message map of the most recent several error runs
  @Nonnull
  private final TreeMap<Long, String> _mostRecentErrorRunMessage;
  // the timestamp of the most recent several success runs
  @Nonnull
  private final List<Long> _mostRecentSuccessRunTS;
  private final int _version;

  private TaskGeneratorMostRecentRunInfo(String tableNameWithType, String taskType,
      Map<Long, String> mostRecentErrorRunMessage, List<Long> mostRecentSuccessRunTS, int version) {
    _tableNameWithType = tableNameWithType;
    _taskType = taskType;
    // sort and keep the most recent several error messages
    _mostRecentErrorRunMessage = new TreeMap<>();
    mostRecentErrorRunMessage.forEach((k, v) -> {
      _mostRecentErrorRunMessage.put(k, v);
      if (_mostRecentErrorRunMessage.size() > MAX_NUM_OF_HISTORY_TO_KEEP) {
        _mostRecentErrorRunMessage.remove(_mostRecentErrorRunMessage.firstKey());
      }
    });
    // sort and keep the most recent several timestamp
    Queue<Long> sortedTs = new PriorityQueue<>();
    mostRecentSuccessRunTS.forEach(e -> {
      sortedTs.offer(e);
      if (sortedTs.size() > MAX_NUM_OF_HISTORY_TO_KEEP) {
        sortedTs.poll();
      }
    });
    _mostRecentSuccessRunTS = new ArrayList<>();
    while (!sortedTs.isEmpty()) {
      _mostRecentSuccessRunTS.add(sortedTs.poll());
    }
    _version = version;
  }

  /**
   * Returns the table name with type
   */
  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  @Override
  public String getTaskType() {
    return _taskType;
  }

  /**
   * Gets the timestamp to error message map of the most recent several error runs
   */
  public TreeMap<Long, String> getMostRecentErrorRunMessage() {
    return _mostRecentErrorRunMessage;
  }

  /**
   * Adds an error run message
   * @param ts A timestamp
   * @param message An error message.
   */
  public void addErrorRunMessage(long ts, String message) {
    _mostRecentErrorRunMessage.put(ts, message);
    if (_mostRecentErrorRunMessage.size() > MAX_NUM_OF_HISTORY_TO_KEEP) {
      _mostRecentErrorRunMessage.remove(_mostRecentErrorRunMessage.firstKey());
    }
  }

  /**
   * Gets the timestamp of the most recent several success runs
   */
  public List<Long> getMostRecentSuccessRunTS() {
    return _mostRecentSuccessRunTS;
  }

  /**
   * Adds a success task generating run timestamp
   * @param ts A timestamp
   */
  public void addSuccessRunTs(long ts) {
    _mostRecentSuccessRunTS.add(ts);
    if (_mostRecentSuccessRunTS.size() > MAX_NUM_OF_HISTORY_TO_KEEP) {
      // sort first in case the given timestamp is not the largest one.
      Collections.sort(_mostRecentSuccessRunTS);
      _mostRecentSuccessRunTS.remove(0);
    }
  }

  /**
   * Returns the current information version, it should be consistent with the corresponding {@link ZNRecord} version.
   */
  public int getVersion() {
    return _version;
  }

  /**
   * Creates a new empty {@link TaskGeneratorMostRecentRunInfo}
   * @param tableNameWithType the table name with type
   * @param taskType the task type.
   * @return a new empty {@link TaskGeneratorMostRecentRunInfo}
   */
  public static TaskGeneratorMostRecentRunInfo newInstance(String tableNameWithType, String taskType) {
    return new TaskGeneratorMostRecentRunInfo(tableNameWithType, taskType, Collections.EMPTY_MAP,
        Collections.EMPTY_LIST, -1);
  }
}
