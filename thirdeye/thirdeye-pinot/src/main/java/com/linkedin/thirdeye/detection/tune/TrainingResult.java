/*
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

package com.linkedin.thirdeye.detection.tune;

import java.util.HashMap;
import java.util.Map;

/**
 * Training result
 */
public class TrainingResult {
  // specs of this stage
  private Map<String, Object> stageSpecs;

  // specs for the baseline provider
  private Map<String, Object> baselineProviderSpecs;

  public TrainingResult() {
    this.stageSpecs = new HashMap<>();
    this.baselineProviderSpecs = new HashMap<>();
  }

  private TrainingResult(Map<String, Object> stageSpecs, Map<String, Object> baselineProviderSpecs) {
    this.stageSpecs = stageSpecs;
    this.baselineProviderSpecs = baselineProviderSpecs;
  }

  public Map<String, Object> getStageSpecs() {
    return stageSpecs;
  }

  public Map<String, Object> getBaselineProviderSpecs() {
    return baselineProviderSpecs;
  }

  public TrainingResult withStageSpecs(Map<String, Object> stageSpecs) {
    return new TrainingResult(stageSpecs, this.baselineProviderSpecs);
  }

  public TrainingResult withBaselineSpecs(Map<String, Object> baselineProviderSpecs) {
    return new TrainingResult(this.stageSpecs, baselineProviderSpecs);
  }

}
