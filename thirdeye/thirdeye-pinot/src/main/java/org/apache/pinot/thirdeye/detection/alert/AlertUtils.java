/*
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

package org.apache.pinot.thirdeye.detection.alert;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.mail.internet.InternetAddress;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;


public class AlertUtils {
  private AlertUtils() {
    //left blank
  }

  /**
   * Helper to determine presence of user-feedback for an anomaly
   *
   * @param anomaly anomaly
   * @return {@code true} if feedback exists and is TRUE or FALSE, {@code false} otherwise
   */
  public static boolean hasFeedback(MergedAnomalyResultDTO anomaly) {
    return anomaly.getFeedback() != null
        && !anomaly.getFeedback().getFeedbackType().isUnresolved();
  }

  /**
   * Helper to convert a collection of email strings into {@code InternetAddress} instances, filtering
   * out invalid addresses and nulls.
   *
   * @param emailCollection collection of email address strings
   * @return filtered collection of InternetAddress objects
   */
  public static Collection<InternetAddress> toAddress(Collection<String> emailCollection) {
    if (CollectionUtils.isEmpty(emailCollection)) {
      return Collections.emptySet();
    }
    return Collections2.filter(Collections2.transform(emailCollection,
        new Function<String, InternetAddress>() {
          @Override
          public InternetAddress apply(String s) {
            try {
              return InternetAddress.parse(s)[0];
            } catch (Exception e) {
              return null;
            }
          }
        }),
        new Predicate<InternetAddress>() {
          @Override
          public boolean apply(InternetAddress internetAddress) {
            return internetAddress != null;
          }
        });
  }

  private static long getLastTimeStamp(Collection<MergedAnomalyResultDTO> anomalies, long startTime) {
    long lastTimeStamp = startTime;
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      lastTimeStamp = Math.max(anomaly.getCreatedTime(), lastTimeStamp);
    }
    return lastTimeStamp;
  }

  public static Map<Long, Long> makeVectorClock(Collection<MergedAnomalyResultDTO> anomalies) {
    Multimap<Long, MergedAnomalyResultDTO> grouped = Multimaps.index(anomalies, new Function<MergedAnomalyResultDTO, Long>() {
      @Nullable
      @Override
      public Long apply(@Nullable MergedAnomalyResultDTO mergedAnomalyResultDTO) {
        // Return functionId to support alerting of legacy anomalies
        if (mergedAnomalyResultDTO.getDetectionConfigId() == null) {
          return mergedAnomalyResultDTO.getFunctionId();
        }

        return mergedAnomalyResultDTO.getDetectionConfigId();
      }
    });
    Map<Long, Long> detection2max = new HashMap<>();
    for (Map.Entry<Long, Collection<MergedAnomalyResultDTO>> entry : grouped.asMap().entrySet()) {
      detection2max.put(entry.getKey(), getLastTimeStamp(entry.getValue(), -1));
    }
    return detection2max;
  }

  public static Map<Long, Long> mergeVectorClock(Map<Long, Long> a, Map<Long, Long> b) {
    Set<Long> keySet = new HashSet<>();
    if (a != null) {
      keySet.addAll(a.keySet());
    }
    if (b != null) {
      keySet.addAll(b.keySet());
    }

    Map<Long, Long> result = new HashMap<>();
    for (Long detectionId : keySet) {
      long valA = MapUtils.getLongValue(a, detectionId, -1);
      long valB = MapUtils.getLongValue(b, detectionId, -1);
      result.put(detectionId, Math.max(valA, valB));
    }

    return result;
  }

}
