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
import java.sql.Timestamp;
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


public class UnnotifiedAnomalyFetcher extends BaseAnomalyFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(UnnotifiedAnomalyFetcher.class);

  public static final String MAXIMUM_ANOMALY_LOOK_BACK_LENGTH = "maxAnomalyLookBack";

  public static final String DEFAULT_MAXIMUM_ANOMALY_LOOK_BACK_LENGTH = "1_DAYS";

  public UnnotifiedAnomalyFetcher(){
    super();
  }

  /**
   * Fetch the list of un-notified anomalies, whose create time is after last notify time
   * @param current the current DateTime
   * @param alertSnapShot the snapshot of the AnomalyFeed
   * @return a list of un-notified anomalies
   */
  @Override
  public Collection<MergedAnomalyResultDTO> getAlertCandidates(DateTime current, AlertSnapshotDTO alertSnapShot) {
    if (!this.active) {
      LOG.warn("UnnotifiedAnomalyFetcher is not active for fetching anomalies");
      return Collections.emptyList();
    }
    if (StringUtils.isBlank(this.anomalyFetcherConfig.getAnomalySource()) ||
        this.anomalyFetcherConfig.getAnomalySourceType() == null) {
      LOG.error("No entry of {} or {} in the AnomalyFetcherConfig", ANOMALY_SOURCE_TYPE, ANOMALY_SOURCE);
      return Collections.emptyList();
    }

    Period maxAnomalyLookBack = TimeGranularity.fromString(
        this.properties.getProperty(MAXIMUM_ANOMALY_LOOK_BACK_LENGTH, DEFAULT_MAXIMUM_ANOMALY_LOOK_BACK_LENGTH)).toPeriod();

    // Fetch anomalies who are created MAXIMUM_ANOMALY_LOOK_BACK_LENGTH ago
    AnomalySource anomalySourceType = anomalyFetcherConfig.getAnomalySourceType();
    String anomalySource = anomalyFetcherConfig.getAnomalySource();
    Predicate predicate = Predicate.AND(anomalySourceType.getPredicate(anomalySource),
        Predicate.GE("createTime",
            new Timestamp(current.minus(maxAnomalyLookBack).getMillis())));
    Set<MergedAnomalyResultDTO> alertCandidates = new HashSet<>(mergedAnomalyResultDAO.findByPredicate(predicate));

    // parse snapshot to a map, getting the last notified time of given metric::dimension pair
    Multimap<String, AnomalyNotifiedStatus> snapshot = alertSnapShot.getSnapshot();
    if (snapshot.size() == 0) {
      return new ArrayList<>(alertCandidates);
    }

    // filter out alert candidates by snapshot
    Iterator<MergedAnomalyResultDTO> iterator = alertCandidates.iterator();
    while (iterator.hasNext()) {
      MergedAnomalyResultDTO mergedAnomaly = iterator.next();
      String snapshotKey = BaseAnomalyFetcher.getSnapshotKey(mergedAnomaly);

      if (snapshot.containsKey(snapshotKey)) {
        // If the mergedAnomaly's create time is before last notify time, discard
        long lastNotifyTime = alertSnapShot.getLatestStatus(snapshot, snapshotKey).getLastNotifyTime();
        if (mergedAnomaly.getCreatedTime() < lastNotifyTime) {
          iterator.remove();
        }
      }
    }

    return alertCandidates;
  }
}
