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

package com.linkedin.thirdeye.anomalydetection.context;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;

public interface AnomalyFeedback {
  /**
   * Set feedback type (e.g., anomaly, anomaly no action, etc.)
   * @param feedbackType feedback type
   */
  void setFeedbackType(AnomalyFeedbackType feedbackType);

  /**
   * Get feedback type (e.g., anomaly, anomaly no action, etc.)
   * @return feedback type
   */
  AnomalyFeedbackType getFeedbackType();

  /**
   * Set comment for this feedback.
   * @param comment comment for this feedback.
   */
  void setComment(String comment);

  /**
   * Get comment of this feedback.
   * @return comment of this feedback.
   */
  String getComment();
}
