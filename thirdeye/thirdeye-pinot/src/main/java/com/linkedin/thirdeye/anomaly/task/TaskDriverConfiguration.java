/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.anomaly.task;

public class TaskDriverConfiguration {
  private int noTaskDelayInMillis = 15_000; // 15 seconds
  private int taskFailureDelayInMillis = 30_000; // 30 seconds
  private int randomDelayCapInMillis = 15_000; // 15 seconds
  private int taskFetchSizeCap = 50;
  private int maxParallelTasks = 5;

  public int getNoTaskDelayInMillis() {
    return noTaskDelayInMillis;
  }

  public void setNoTaskDelayInMillis(int noTaskDelayInMillis) {
    this.noTaskDelayInMillis = noTaskDelayInMillis;
  }

  public int getTaskFailureDelayInMillis() {
    return taskFailureDelayInMillis;
  }

  public void setTaskFailureDelayInMillis(int taskFailureDelayInMillis) {
    this.taskFailureDelayInMillis = taskFailureDelayInMillis;
  }

  public int getRandomDelayCapInMillis() {
    return randomDelayCapInMillis;
  }

  public void setRandomDelayCapInMillis(int randomDelayCapInMillis) {
    this.randomDelayCapInMillis = randomDelayCapInMillis;
  }

  public int getTaskFetchSizeCap() {
    return taskFetchSizeCap;
  }

  public void setTaskFetchSizeCap(int taskFetchSizeCap) {
    this.taskFetchSizeCap = taskFetchSizeCap;
  }

  public int getMaxParallelTasks() {
    return maxParallelTasks;
  }

  public void setMaxParallelTasks(int maxParallelTasks) {
    this.maxParallelTasks = maxParallelTasks;
  }
}
