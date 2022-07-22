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

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class InMemoryTaskManagerStatusCache implements TaskManagerStatusCache<TaskGeneratorMostRecentRunInfo> {

  private static class TaskGeneratorCacheKey {
    final String _tableNameWithType;
    final String _taskType;

    private TaskGeneratorCacheKey(String tableNameWithType, String taskType) {
      _tableNameWithType = tableNameWithType;
      _taskType = taskType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TaskGeneratorCacheKey that = (TaskGeneratorCacheKey) o;
      return _tableNameWithType.equals(that._tableNameWithType) && _taskType.equals(that._taskType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_tableNameWithType, _taskType);
    }
  }

  private final ConcurrentHashMap<TaskGeneratorCacheKey, TaskGeneratorMostRecentRunInfo> _cacheMap;

  public InMemoryTaskManagerStatusCache() {
    _cacheMap = new ConcurrentHashMap<>();
  }

  @Override
  public TaskGeneratorMostRecentRunInfo fetchTaskGeneratorInfo(String tableNameWithType, String taskType) {
    return _cacheMap.get(new TaskGeneratorCacheKey(tableNameWithType, taskType));
  }

  @Override
  public void saveTaskGeneratorInfo(String tableNameWithType, String taskType,
      Consumer<TaskGeneratorMostRecentRunInfo> taskGeneratorMostRecentRunInfoUpdater) {
    _cacheMap.compute(new TaskGeneratorCacheKey(tableNameWithType, taskType), (k, v) -> {
      if (v == null) {
        v = TaskGeneratorMostRecentRunInfo.newInstance(tableNameWithType, taskType);
      }
      taskGeneratorMostRecentRunInfoUpdater.accept(v);
      return v;
    });
  }

  @Override
  public void deleteTaskGeneratorInfo(String tableNameWithType, String taskType) {
    _cacheMap.remove(new TaskGeneratorCacheKey(tableNameWithType, taskType));
  }
}
