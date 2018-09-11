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

package com.linkedin.thirdeye.alert.feed;

import com.linkedin.thirdeye.alert.commons.AnomalyFeedConfig;
import com.linkedin.thirdeye.alert.commons.AnomalyFetcherConfig;
import com.linkedin.thirdeye.alert.commons.AnomalyFetcherFactory;
import com.linkedin.thirdeye.alert.commons.AnomalySource;
import com.linkedin.thirdeye.alert.fetcher.AnomalyFetcher;
import com.linkedin.thirdeye.anomaly.alert.util.AlertFilterHelper;
import com.linkedin.thirdeye.datalayer.bao.AlertSnapshotManager;
import com.linkedin.thirdeye.datalayer.dto.AlertSnapshotDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.Set;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UnionAnomalyFeed implements AnomalyFeed {
  private static final Logger LOG = LoggerFactory.getLogger(UnionAnomalyFeed.class);

  private AlertSnapshotManager alertSnapshotDAO;
  private AlertFilterFactory alertFilterFactory;

  private List<AnomalyFetcher> anomalyFetchers;
  private List<AlertFilter> alertCandidatesFilters;
  private AlertSnapshotDTO alertSnapshot;
  private DateTime alertTime;

  public UnionAnomalyFeed(){
    this.anomalyFetchers = new ArrayList<>();
    this.alertCandidatesFilters = new ArrayList<>();
    this.alertTime = DateTime.now();
  }

  @Override
  public void init(AlertFilterFactory alertFilterFactory, AnomalyFeedConfig anomalyFeedConfig) {
    this.alertSnapshotDAO = DAORegistry.getInstance().getAlertSnapshotDAO();
    this.alertFilterFactory = alertFilterFactory;
    AnomalySource anomalySourceType = anomalyFeedConfig.getAnomalySourceType();
    String anomalySource = anomalyFeedConfig.getAnomalySource();
    if (anomalyFeedConfig.getAlertSnapshotId() != null) {
      alertSnapshot = alertSnapshotDAO.findById(anomalyFeedConfig.getAlertSnapshotId());
    }
    if (alertSnapshot == null) { // null handling
      LOG.error("Alert snapshot is null.");
      throw new NullPointerException("Alert snapshot is null");
    }
    List<AnomalyFetcherConfig> anomalyFetcherSpecs = anomalyFeedConfig.getAnomalyFetcherConfigs();
    List<Map<String, String>> alertFilterSpecs = anomalyFeedConfig.getAlertFilterConfigs();

    for (AnomalyFetcherConfig anomalyFetcherSpec : anomalyFetcherSpecs) {
      AnomalyFetcher anomalyFetcher;
      try {
        anomalyFetcher = AnomalyFetcherFactory.fromClassName(anomalyFetcherSpec.getType());
      } catch (Exception e) {
        LOG.error("Cannot instantiate anomaly fetcher {}", anomalyFetcherSpec.getType(), e);
        continue;
      }

      anomalyFetcherSpec.setAnomalySourceType(anomalySourceType);
      anomalyFetcherSpec.setAnomalySource(anomalySource);
      anomalyFetcher.init(anomalyFetcherSpec);
      anomalyFetchers.add(anomalyFetcher);
    }

    for(Map<String, String> alertFilterSpec : alertFilterSpecs) {
      alertCandidatesFilters.add(alertFilterFactory.fromSpec(alertFilterSpec));
    }
  }

  @Override
  public Collection<MergedAnomalyResultDTO> getAnomalyFeed() {
    Set<MergedAnomalyResultDTO> mergedAnomalyResultSet = new HashSet<>();
    for (AnomalyFetcher anomalyFetcher : anomalyFetchers) {
      mergedAnomalyResultSet.addAll(anomalyFetcher.getAlertCandidates(alertTime, alertSnapshot));
    }

    // If mergedAnomalyResultSet is empty, then no act after that.
    if (mergedAnomalyResultSet.isEmpty()) {
      return mergedAnomalyResultSet;
    }

    List<MergedAnomalyResultDTO> mergedAnomalyResults = new ArrayList<>(mergedAnomalyResultSet);
    mergedAnomalyResults = AlertFilterHelper.applyFiltrationRule(mergedAnomalyResults, alertFilterFactory);

    for(AlertFilter alertFilter : alertCandidatesFilters) {
      Iterator<MergedAnomalyResultDTO> mergedAnomalyIterator = mergedAnomalyResults.iterator();
      while (mergedAnomalyIterator.hasNext()) {
        MergedAnomalyResultDTO mergedAnomalyResult = mergedAnomalyIterator.next();
        if (!alertFilter.isQualified(mergedAnomalyResult)) {
          mergedAnomalyIterator.remove();
        }
      }
    }
    return mergedAnomalyResults;
  }

  @Override
  public void updateSnapshot(Collection<MergedAnomalyResultDTO> alertedAnomalies) {
    if (alertSnapshot == null) {
      LOG.error("No alertSnapshot was initiated.");
      throw new NullPointerException("Alert snapshot is null");
    }

    alertSnapshot.updateSnapshot(alertTime, new ArrayList<>(alertedAnomalies));
    alertSnapshotDAO.update(alertSnapshot);
  }
}
