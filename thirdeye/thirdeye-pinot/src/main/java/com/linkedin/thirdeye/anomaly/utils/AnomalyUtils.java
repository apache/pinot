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

package com.linkedin.thirdeye.anomaly.utils;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AnomalyUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyUtils.class);

  /**
   * Logs the known anomalies whose window overlaps with the given window, whose range is defined by windowStart
   * and windowEnd.
   *
   * Reason to log the overlapped anomalies: During anomaly detection, the know anomalies are supposedly used to remove
   * abnormal baseline values but not current values. This method provides a check before sending the known anomalies to
   * anomaly detection functions.
   *
   * @param windowStart the inclusive start time of the window
   * @param windowEnd the exclusive end time of the window
   * @param knownAnomalies the known anomalies
   */
  public static void logAnomaliesOverlapWithWindow(DateTime windowStart, DateTime windowEnd,
      List<MergedAnomalyResultDTO> knownAnomalies) {
    if (CollectionUtils.isEmpty(knownAnomalies) || windowEnd.compareTo(windowStart) <= 0) {
      return;
    }

    List<MergedAnomalyResultDTO> overlappedAnomalies = new ArrayList<>();
    for (MergedAnomalyResultDTO knownAnomaly : knownAnomalies) {
      if (knownAnomaly.getStartTime() <= windowEnd.getMillis() && knownAnomaly.getEndTime() >= windowStart.getMillis()) {
        overlappedAnomalies.add(knownAnomaly);
      }
    }

    if (overlappedAnomalies.size() > 0) {
      StringBuffer sb = new StringBuffer();
      String separator = "";
      for (MergedAnomalyResultDTO overlappedAnomaly : overlappedAnomalies) {
        sb.append(separator).append(overlappedAnomaly.getStartTime()).append("--").append(overlappedAnomaly.getEndTime());
        separator = ", ";
      }
      LOG.warn("{} merged anomalies overlap with this window {} -- {}. Anomalies: {}", overlappedAnomalies.size(),
          windowStart, windowEnd, sb.toString());
    }
  }

  /**
   * This function checks if the input list of merged anomalies has at least one positive label.
   * It is a helper for alert filter auto tuning
   * @param mergedAnomalyResultDTOS
   * @return true if the list of merged anomalies has at least one positive label, false otherwise
   */
  public static Boolean checkHasLabels(List<MergedAnomalyResultDTO> mergedAnomalyResultDTOS){
    for(MergedAnomalyResultDTO anomaly: mergedAnomalyResultDTOS){
      AnomalyFeedback feedback = anomaly.getFeedback();
      if (feedback != null){
        return true;
      }
    }
    return false;
  }

  /**
   * Safely and quietly shutdown executor service. This method waits until all threads are complete,
   * or timeout occurs (5-minutes), or the current thread is interrupted, whichever happens first.
   *
   * @param executorService the executor service to be shutdown.
   * @param ownerClass the class that owns the executor service; it could be null.
   */
  public static void safelyShutdownExecutionService(ExecutorService executorService, Class ownerClass) {
    safelyShutdownExecutionService(executorService, 300, ownerClass);
  }

  /**
   * Safely and quietly shutdown executor service. This method waits until all threads are complete,
   * or number of retries is reached, or the current thread is interrupted, whichever happens first.
   *
   * @param executorService the executor service to be shutdown.
   * @param maxWaitTimeInSeconds max wait time for threads that are still running.
   * @param ownerClass the class that owns the executor service; it could be null.
   */
  public static void safelyShutdownExecutionService(ExecutorService executorService, int maxWaitTimeInSeconds,
      Class ownerClass) {
    if (executorService == null) {
      return;
    }
    executorService.shutdown(); // Prevent new tasks from being submitted
    try {
      // If not all threads are complete, then a retry loop waits until all threads are complete, or timeout occurs,
      // or the current thread is interrupted, whichever happens first.
      for (int retryCount = 0; retryCount < maxWaitTimeInSeconds; ++retryCount) {
        // Wait a while for existing tasks to terminate
        if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
          // Force terminate all currently executing tasks if they support such operation
          executorService.shutdownNow();
          if (retryCount % 10 == 0) {
            if (ownerClass != null) {
              LOG.info("Trying to terminate thread pool for class {}", ownerClass.getSimpleName());
            } else {
              LOG.info("Trying to terminate thread pool: {}.", executorService);
            }
          }
        } else {
          break; // break out retry loop if all threads are complete
        }
      }
    } catch (InterruptedException e) { // If current thread is interrupted
      executorService.shutdownNow(); // Interrupt all currently executing tasks for the last time
      Thread.currentThread().interrupt();
    }
  }

  /**
   * This is a subclass describing anomalies features as training data for alert filter
   */
  public static class MetaDataNode {
    public double windowSize;
    public double severity;
    public String startTimeISO;
    public String endTimeISO;
    public String functionName;
    public String feedback;
    public long anomalyId;

    public MetaDataNode(MergedAnomalyResultDTO anomaly){
      this.windowSize = 1. * (anomaly.getEndTime() - anomaly.getStartTime()) / 3600000L;
      this.severity = anomaly.getWeight();
      this.startTimeISO = new Timestamp(anomaly.getStartTime()).toString();
      this.endTimeISO = new Timestamp(anomaly.getEndTime()).toString();
      this.functionName = anomaly.getFunction().getFunctionName();
      this.feedback = (anomaly.getFeedback() == null)? "null" : String.valueOf(anomaly.getFeedback().getFeedbackType());
      this.anomalyId = anomaly.getId();
    }
  }
}
