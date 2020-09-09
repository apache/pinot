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

package org.apache.pinot.thirdeye.alert.fetcher;

import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.alert.commons.AnomalyNotifiedStatus;
import org.apache.pinot.thirdeye.alert.commons.AnomalySource;
import org.apache.pinot.thirdeye.datalayer.dto.AlertSnapshotDTO;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ContinuumAnomalyFetcher extends BaseAnomalyFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(ContinuumAnomalyFetcher.class);

  public static final String REALERT_FREQUENCY = "realertFrequency";

  public static final String DEFAULT_REALERT_FREQUENCY = "1_DAYS";

  public ContinuumAnomalyFetcher(){
    super();
  }

  /**
   * Fetch continuum anomalies whose end time is after last notify time; that is, the anomaly continues after last alert
   * @param current current DateTime
   * @param alertSnapShot the snapshot of the AnomalyFeed
   * @return a list of continuing merged anomalies
   */
  @Override
  public Collection<MergedAnomalyResultDTO> getAlertCandidates(DateTime current, AlertSnapshotDTO alertSnapShot) {
    if (!this.active) {
      LOG.warn("ContinuumAnomalyFetcher is not active for fetching anomalies");
      return Collections.emptyList();
    }
    if (StringUtils.isBlank(this.anomalyFetcherConfig.getAnomalySource()) ||
        this.anomalyFetcherConfig.getAnomalySourceType() == null) {
      LOG.error("No entry of {} or {} in the AnomalyFetcherConfig", ANOMALY_SOURCE_TYPE, ANOMALY_SOURCE);
      return Collections.emptyList();
    }

    Period realertFrequency = TimeGranularity.fromString(
        this.properties.getProperty(REALERT_FREQUENCY, DEFAULT_REALERT_FREQUENCY)).toPeriod();
    long maxAnomalyRealertTimestamp = current.minus(realertFrequency).getMillis();

    long lastNotifyTime = Math.max(alertSnapShot.getLastNotifyTime(), maxAnomalyRealertTimestamp);
    AnomalySource anomalySourceType = anomalyFetcherConfig.getAnomalySourceType();
    String anomalySource = anomalyFetcherConfig.getAnomalySource();
    Predicate predicate = Predicate.AND(anomalySourceType.getPredicate(anomalySource),
        Predicate.GE("endTime", lastNotifyTime));
    Set<MergedAnomalyResultDTO> alertCandidates = new HashSet<>(mergedAnomalyResultDAO.findByPredicate(predicate));

    Multimap<String, AnomalyNotifiedStatus> snapshot = alertSnapShot.getSnapshot();
    if (snapshot.size() == 0) {
      return new ArrayList<>(alertCandidates);
    }

    // filter out alert candidates by snapshot
    Iterator<MergedAnomalyResultDTO> alertCandidatesIterator = alertCandidates.iterator();
    while (alertCandidatesIterator.hasNext()) {
      MergedAnomalyResultDTO mergedAnomaly = alertCandidatesIterator.next();
      if (mergedAnomaly.getStartTime() > alertSnapShot.getLastNotifyTime()) {
        // this anomaly start after last notify time, pass without check
        continue;
      }

      String snapshotKey = BaseAnomalyFetcher.getSnapshotKey(mergedAnomaly);
      if (snapshot.containsKey(snapshotKey)){
        // If the mergedAnomaly's start time is before last notify time and
        // the last notify time of the metric-dimension isn't REALERT_FREQUENCY ahead, discard
        long metricLastNotifyTime = alertSnapShot.getLatestStatus(snapshot, snapshotKey).getLastNotifyTime();
        if (mergedAnomaly.getStartTime() < metricLastNotifyTime &&
            metricLastNotifyTime < current.minus(realertFrequency).getMillis()) {
          alertCandidatesIterator.remove();
        }
      }
    }

    return alertCandidates;
  }
}
