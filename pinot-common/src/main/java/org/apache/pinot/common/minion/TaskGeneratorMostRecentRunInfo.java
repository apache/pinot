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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;


/**
 * a task generator running history which keeps the most recent several success run timestamp and the most recent
 * several error run messages.
 */
@JsonPropertyOrder({"tableNameWithType", "taskType", "mostRecentSuccessRunTS", "mostRecentErrorRunMessages"})
public class TaskGeneratorMostRecentRunInfo extends BaseTaskGeneratorInfo {
  @VisibleForTesting
  static final int MAX_NUM_OF_HISTORY_TO_KEEP = 5;
  private final String _taskType;
  private final String _tableNameWithType;
  // the timestamp to error message map of the most recent several error runs
  private final TreeMap<Long, String> _mostRecentErrorRunMessages;
  // the timestamp of the most recent several success runs
  private final List<Long> _mostRecentSuccessRunTS;

  private TaskGeneratorMostRecentRunInfo(String tableNameWithType, String taskType) {
    _tableNameWithType = tableNameWithType;
    _taskType = taskType;
    _mostRecentErrorRunMessages = new TreeMap<>();
    _mostRecentSuccessRunTS = new ArrayList<>();
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
  public TreeMap<String, String> getMostRecentErrorRunMessages() {
    TreeMap<String, String> result = new TreeMap<>();
    _mostRecentErrorRunMessages.forEach((timestamp, error) -> result.put(
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC).toString(),
        error));
    return result;
  }

  /**
   * Adds an error run message
   * @param ts A timestamp
   * @param message An error message.
   */
  public void addErrorRunMessage(long ts, String message) {
    _mostRecentErrorRunMessages.put(ts, message);
    if (_mostRecentErrorRunMessages.size() > MAX_NUM_OF_HISTORY_TO_KEEP) {
      _mostRecentErrorRunMessages.remove(_mostRecentErrorRunMessages.firstKey());
    }
  }

  /**
   * Gets the timestamp of the most recent several success runs
   */
  public List<String> getMostRecentSuccessRunTS() {
    return _mostRecentSuccessRunTS.stream().map(
            timestamp -> OffsetDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC)
                .toString())
        .collect(Collectors.toList());
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
   * Creates a new empty {@link TaskGeneratorMostRecentRunInfo}
   * @param tableNameWithType the table name with type
   * @param taskType the task type.
   * @return a new empty {@link TaskGeneratorMostRecentRunInfo}
   */
  public static TaskGeneratorMostRecentRunInfo newInstance(String tableNameWithType, String taskType) {
    return new TaskGeneratorMostRecentRunInfo(tableNameWithType, taskType);
  }
}
