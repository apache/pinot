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
 *
 */

package org.apache.pinot.thirdeye.dashboard.resources.v2.anomalies;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.constant.AnomalyFeedbackType;
import org.apache.pinot.thirdeye.dashboard.resources.v2.ResourceUtils;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.AbstractDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.DetectionConfigBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;

import static org.apache.pinot.thirdeye.constant.AnomalyFeedbackType.*;


/**
 * The type Anomaly searcher.
 */
public class AnomalySearcher {
  private final MergedAnomalyResultManager anomalyDAO;
  private final DetectionConfigManager detectionConfigDAO;
  private final DetectionAlertConfigManager detectionAlertConfigDAO;

  /**
   * Instantiates a new Anomaly searcher.
   */
  public AnomalySearcher() {
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.detectionConfigDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.detectionAlertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
  }

  /**
   * Search and retrieve all the anomalies matching to the search filter and limits.
   *
   * @param searchFilter the search filter
   * @param limit the limit
   * @param offset the offset
   * @return the result
   */
  public Map<String, Object> search(AnomalySearchFilter searchFilter, int limit, int offset) {
    // default dummy predicate, the base id can't be 0
    Predicate predicate = Predicate.NEQ("baseId", 0);
    // hide child anomaly unless queried with ids
    if (searchFilter.getAnomalyIds().isEmpty()) {
      predicate = Predicate.EQ("child", false);
    }
    if (searchFilter.getStartTime() != null) {
      predicate = Predicate.AND(predicate, Predicate.LT("startTime", searchFilter.getEndTime()));
    }
    if (searchFilter.getEndTime() != null) {
      predicate = Predicate.AND(predicate, Predicate.GT("endTime", searchFilter.getStartTime()));
    }
    // search by detections or subscription groups
    Set<Long> detectionConfigIds = new HashSet<>();
    Set<Long> subscribedDetectionConfigIds = new HashSet<>();
    if (!searchFilter.getDetectionNames().isEmpty()) {
      detectionConfigIds =
          this.detectionConfigDAO.findByPredicate(Predicate.IN("name", searchFilter.getDetectionNames().toArray()))
              .stream()
              .map(DetectionConfigBean::getId)
              .collect(Collectors.toSet());
    }
    if (!searchFilter.getSubscriptionGroups().isEmpty()) {
      subscribedDetectionConfigIds = this.detectionAlertConfigDAO.findByPredicate(
          Predicate.IN("name", searchFilter.getSubscriptionGroups().toArray()))
          .stream()
          .map(detectionAlertConfigDTO -> detectionAlertConfigDTO.getVectorClocks().keySet())
          .flatMap(Collection::stream)
          .collect(Collectors.toSet());
    }
    if (!searchFilter.getDetectionNames().isEmpty() && !searchFilter.getSubscriptionGroups().isEmpty()) {
      // intersect the detection config ids if searching by both
      detectionConfigIds.retainAll(subscribedDetectionConfigIds);
    } else {
      detectionConfigIds.addAll(subscribedDetectionConfigIds);
    }
    if (!searchFilter.getDetectionNames().isEmpty() || !searchFilter.getSubscriptionGroups().isEmpty()) {
      // add the predicate using detection config id
      if (detectionConfigIds.isEmpty()) {
        // if detection not found, return empty result
        return ImmutableMap.of("count", 0, "limit", limit, "offset", offset, "elements", Collections.emptyList());
      }
      predicate = Predicate.AND(predicate, Predicate.IN("detectionConfigId", detectionConfigIds.toArray()));
    }

    // search by datasets
    if (!searchFilter.getDatasets().isEmpty()) {
      List<Predicate> datasetPredicates = new ArrayList<>();
      for (String dataset : searchFilter.getDatasets()) {
        datasetPredicates.add(Predicate.LIKE("collection", "%" + dataset + "%"));
      }
      predicate = Predicate.AND(predicate, Predicate.OR(datasetPredicates.toArray(new Predicate[0])));
    }
    // search by metrics
    if (!searchFilter.getMetrics().isEmpty()) {
      predicate = Predicate.AND(predicate, Predicate.IN("metric", searchFilter.getMetrics().toArray()));
    }
    // search by ids
    if (!searchFilter.getAnomalyIds().isEmpty()) {
      predicate = Predicate.AND(predicate, Predicate.IN("baseId", searchFilter.getAnomalyIds().toArray()));
    }

    long count;
    List<MergedAnomalyResultDTO> results;
    if (searchFilter.getFeedbacks().isEmpty()) {
      List<Long> anomalyIds = this.anomalyDAO.findIdsByPredicate(predicate)
          .stream()
          .sorted(Comparator.reverseOrder())
          .collect(Collectors.toList());
      count = anomalyIds.size();
      results = anomalyIds.isEmpty() ? Collections.emptyList()
          : this.anomalyDAO.findByIds(ResourceUtils.paginateResults(anomalyIds, offset, limit));
    } else {
      // filter by feedback types if requested
      List<MergedAnomalyResultDTO> anomalies = this.anomalyDAO.findByPredicate(predicate);
      Set<AnomalyFeedbackType> feedbackFilters =
          searchFilter.getFeedbacks().stream().map(AnomalyFeedbackType::valueOf).collect(Collectors.toSet());
      results = anomalies.stream()
          .filter(anomaly -> (anomaly.getFeedback() == null && feedbackFilters.contains(NO_FEEDBACK)) || (
              anomaly.getFeedback() != null && feedbackFilters.contains(anomaly.getFeedback().getFeedbackType())))
          .sorted(Comparator.comparingLong(AbstractDTO::getId).reversed())
          .collect(Collectors.toList());
      count = results.size();
      results = ResourceUtils.paginateResults(results, offset, limit);
    }
    return ImmutableMap.of("count", count, "limit", limit, "offset", offset, "elements", results);
  }
}
