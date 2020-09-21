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

package org.apache.pinot.thirdeye.dashboard.resources.v2.alerts;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.AbstractDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.formatter.DetectionConfigFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Alert searcher.
 */
public class AlertSearcher {
  private static final Logger LOG = LoggerFactory.getLogger(AlertSearcher.class.getName());
  private final DetectionConfigManager detectionConfigDAO;
  private final DetectionAlertConfigManager detectionAlertConfigDAO;
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final DetectionConfigFormatter detectionConfigFormatter;

  /**
   * The Alert search query.
   */
  static class AlertSearchQuery {
    /**
     * The Search filter.
     */
    final AlertSearchFilter searchFilter;
    /**
     * The Limit.
     */
    final long limit;
    /**
     * The Offset.
     */
    final long offset;

    /**
     * Instantiates a new Alert search query.
     *
     * @param searchFilter the search filter
     * @param limit the limit
     * @param offset the offset
     */
    public AlertSearchQuery(AlertSearchFilter searchFilter, long limit, long offset) {
      this.searchFilter = searchFilter;
      this.limit = limit;
      this.offset = offset;
    }
  }

  /**
   * Instantiates a new Alert searcher.
   */
  public AlertSearcher() {
    this.detectionConfigDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.detectionAlertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.detectionConfigFormatter = new DetectionConfigFormatter(metricDAO, datasetDAO);
  }

  /**
   * Search and retrive all the alerts matching to the search filter and limits.
   *
   * @param searchFilter the search filter
   * @param limit the limit
   * @param offset the offset
   * @return the result
   */
  public Map<String, Object> search(AlertSearchFilter searchFilter, long limit, long offset) {
    AlertSearchQuery searchQuery = new AlertSearchQuery(searchFilter, limit, offset);
    List<DetectionAlertConfigDTO> subscriptionGroups = findRelatedSubscriptionGroups(searchQuery);
    List<DetectionConfigDTO> detectionConfigs = findDetectionConfig(searchQuery, subscriptionGroups);
    return getResult(searchQuery, subscriptionGroups, detectionConfigs);
  }

  private List<DetectionAlertConfigDTO> findRelatedSubscriptionGroups(AlertSearchQuery searchQuery) {
    AlertSearchFilter searchFilter = searchQuery.searchFilter;
    if (searchFilter.getApplications().isEmpty() && searchFilter.getSubscriptionGroups().isEmpty()
        && searchFilter.getSubscribedBy().isEmpty()) {
      return this.detectionAlertConfigDAO.findAll();
    }
    List<Predicate> predicates = new ArrayList<>();
    Set<DetectionAlertConfigDTO> subscriptionGroups = new HashSet<>();
    if (!searchFilter.getApplications().isEmpty()) {
      predicates.add(Predicate.IN("application", searchFilter.getApplications().toArray()));
    }
    if (!searchFilter.getSubscriptionGroups().isEmpty()) {
      predicates.add(Predicate.IN("name", searchFilter.getSubscriptionGroups().toArray()));
    }
    if (!predicates.isEmpty()) {
      subscriptionGroups.addAll(
          this.detectionAlertConfigDAO.findByPredicate(Predicate.AND(predicates.toArray(new Predicate[0]))));
    }
    if (!searchFilter.getSubscribedBy().isEmpty()) {
      List<DetectionAlertConfigDTO> jsonValResult = this.detectionAlertConfigDAO.findByPredicateJsonVal(Predicate.OR(
          searchFilter.getSubscribedBy()
              .stream()
              .map(name -> Predicate.LIKE("jsonVal", "%recipients%" + name + "%"))
              .toArray(Predicate[]::new)));
      if (predicates.isEmpty()) {
        subscriptionGroups.addAll(jsonValResult);
      } else {
        // intersect the result from both tables
        subscriptionGroups = jsonValResult.stream().filter(subscriptionGroups::contains).collect(Collectors.toSet());
      }
    }
    return new ArrayList<>(subscriptionGroups);
  }

  private List<DetectionConfigDTO> findDetectionConfig(AlertSearchQuery searchQuery,
      List<DetectionAlertConfigDTO> subscriptionGroups) {
    AlertSearchFilter searchFilter = searchQuery.searchFilter;
    if (searchFilter.isEmpty()) {
      // if no search filter is applied, by default, retrieve the paginated result from db
      return this.detectionConfigDAO.list(searchQuery.limit, searchQuery.offset);
    }

    // look up and run the search filters on the detection config index
    List<DetectionConfigDTO> indexedResult = new ArrayList<>();
    List<Predicate> indexPredicates = new ArrayList<>();
    if (!searchFilter.getApplications().isEmpty() || !searchFilter.getSubscriptionGroups().isEmpty() || !searchFilter.getSubscribedBy().isEmpty()) {
      Set<Long> detectionConfigIds = new TreeSet<>();
      for (DetectionAlertConfigDTO subscriptionGroup : subscriptionGroups) {
        detectionConfigIds.addAll(subscriptionGroup.getVectorClocks().keySet());
      }
      indexPredicates.add(Predicate.IN("baseId", detectionConfigIds.toArray()));
    }
    if (!searchFilter.getCreatedBy().isEmpty()) {
      indexPredicates.add(Predicate.IN("createdBy", searchFilter.getCreatedBy().toArray()));
    }
    if (!searchFilter.getNames().isEmpty()) {
      indexPredicates.add(Predicate.IN("name", searchFilter.getNames().toArray()));
    }
    if (searchFilter.getActive() != null) {
      indexPredicates.add(Predicate.EQ("active", searchFilter.getActive() ? 1 : 0));
    }
    if (!indexPredicates.isEmpty()) {
      indexedResult = this.detectionConfigDAO.findByPredicate(Predicate.AND(indexPredicates.toArray(new Predicate[0])));
    }

    // for metrics, datasets, rule types filters, run the search filters in the generic table
    List<DetectionConfigDTO> jsonValResult = new ArrayList<>();
    List<Predicate> jsonValPredicates = new ArrayList<>();
    if (!searchFilter.getRuleTypes().isEmpty()) {
      List<Predicate> ruleTypePredicates = new ArrayList<>();
      for (String ruleType : searchFilter.getRuleTypes()) {
        ruleTypePredicates.add(Predicate.LIKE("jsonVal", "%componentSpecs%:" + ruleType + "\"%"));
      }
      jsonValPredicates.add(Predicate.OR(ruleTypePredicates.toArray(new Predicate[0])));
    }

    Set<Long> metricIds = new HashSet<>();
    if (!searchFilter.getMetrics().isEmpty()) {
      for (String metric : searchFilter.getMetrics()) {
        metricIds.addAll(
            this.metricDAO.findByMetricName(metric).stream().map(AbstractDTO::getId).collect(Collectors.toSet()));
      }
    }

    if (!searchFilter.getDatasets().isEmpty()) {
      Set<Long> metricIdsFromDataset = new HashSet<>();
      for (String dataset : searchFilter.getDatasets()) {
        metricIdsFromDataset.addAll(this.metricDAO.findByPredicate(Predicate.LIKE("dataset", "%" + dataset + "%"))
            .stream()
            .map(AbstractDTO::getId)
            .collect(Collectors.toSet()));
      }
      if (!searchFilter.getMetrics().isEmpty()) {
        metricIds.retainAll(metricIdsFromDataset);
      } else {
        metricIds = metricIdsFromDataset;
      }
    }

    if (!metricIds.isEmpty()) {
      List<Predicate> metricUrnPredicates = new ArrayList<>();
      for (Long id : metricIds) {
        metricUrnPredicates.add(Predicate.LIKE("jsonVal", "%thirdeye:metric:" + id + "%"));
      }
      jsonValPredicates.add(Predicate.OR(metricUrnPredicates.toArray(new Predicate[0])));
    }

    if (!jsonValPredicates.isEmpty()) {
      jsonValResult =
          this.detectionConfigDAO.findByPredicateJsonVal(Predicate.AND(jsonValPredicates.toArray(new Predicate[0])));
    }

    List<DetectionConfigDTO> result;
    if (!jsonValPredicates.isEmpty() && !indexPredicates.isEmpty()) {
      // merge the result from both tables
      result = jsonValResult.stream().filter(indexedResult::contains).collect(Collectors.toList());
    } else {
      jsonValResult.addAll(indexedResult);
      result = jsonValResult;
    }
    return result.stream().sorted(Comparator.comparingLong(AbstractDTO::getId).reversed()).collect(Collectors.toList());
  }

  /**
   * Format and generate the final search result
   */
  private Map<String, Object> getResult(AlertSearchQuery searchQuery, List<DetectionAlertConfigDTO> subscriptionGroups,
      List<DetectionConfigDTO> detectionConfigs) {
    long count;
    if (searchQuery.searchFilter.isEmpty()) {
      // if not filter is applied, execute count query
      count = this.detectionConfigDAO.count();
    } else {
      // count and limit the filtered results
      count = detectionConfigs.size();
      if (searchQuery.offset >= count) {
        // requested page is out of bound
        detectionConfigs.clear();
      } else {
        detectionConfigs = detectionConfigs.subList((int) searchQuery.offset,
            (int) Math.min(searchQuery.offset + searchQuery.limit, count));
      }
    }

    // format the results
    List<Map<String, Object>> alerts = detectionConfigs.parallelStream().map(config -> {
      try {
        return this.detectionConfigFormatter.format(config);
      } catch (Exception e) {
        LOG.warn("formatting detection config failed {}", config.getId(), e);
        return null;
      }
    }).filter(Objects::nonNull).collect(Collectors.toList());

    // join detections with subscription groups
    Multimap<Long, String> detectionIdToSubscriptionGroups = ArrayListMultimap.create();
    Multimap<Long, String> detectionIdToApplications = ArrayListMultimap.create();
    for (DetectionAlertConfigDTO subscriptionGroup : subscriptionGroups) {
      for (long detectionConfigId : subscriptionGroup.getVectorClocks().keySet()) {
        detectionIdToSubscriptionGroups.put(detectionConfigId, subscriptionGroup.getName());
        detectionIdToApplications.put(detectionConfigId, subscriptionGroup.getApplication());
      }
    }
    for (Map<String, Object> alert : alerts) {
      long id = MapUtils.getLong(alert, "id");
      alert.put("subscriptionGroup", detectionIdToSubscriptionGroups.get(id));
      alert.put("application", new TreeSet<>(detectionIdToApplications.get(id)));
    }

    return ImmutableMap.of("count", count, "limit", searchQuery.limit, "offset", searchQuery.offset, "elements",
        alerts);
  }
}
