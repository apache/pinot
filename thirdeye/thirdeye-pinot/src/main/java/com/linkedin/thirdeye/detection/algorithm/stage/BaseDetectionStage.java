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

package com.linkedin.thirdeye.detection.algorithm.stage;

import java.util.Map;


/**
 * Base detection stage
 */
public interface BaseDetectionStage {
  /**
   * Initialize a detection stage.
   * @param specs specs of this detection stage
   * @param startTime start time of this detection window
   * @param endTime end time of this detection window
   */
  void init(Map<String, Object> specs, Long configId, long startTime, long endTime);
}
