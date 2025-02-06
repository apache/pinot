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
package org.apache.pinot.controller.helix.core.minion;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;


public class TaskSchedulingInfo {
  private List<String> _scheduledTaskNames;
  private final List<String> _generationErrors = new ArrayList<>();
  private final List<String> _schedulingErrors = new ArrayList<>();

  @Nullable
  public List<String> getScheduledTaskNames() {
    return _scheduledTaskNames;
  }

  public TaskSchedulingInfo setScheduledTaskNames(List<String> scheduledTaskNames) {
    _scheduledTaskNames = scheduledTaskNames;
    return this;
  }

  public List<String> getGenerationErrors() {
    return _generationErrors;
  }

  public void addGenerationError(String generationError) {
    _generationErrors.add(generationError);
  }

  public List<String> getSchedulingErrors() {
    return _schedulingErrors;
  }

  public void addSchedulingError(String schedulingError) {
    _schedulingErrors.add(schedulingError);
  }
}
