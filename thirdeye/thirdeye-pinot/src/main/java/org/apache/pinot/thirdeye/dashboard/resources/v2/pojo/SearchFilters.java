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

package org.apache.pinot.thirdeye.dashboard.resources.v2.pojo;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyFeedback;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.constant.AnomalyFeedbackType;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchFilters {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(SearchFilters.class);

  Map<String, List<Long>> statusFilterMap;

  Map<String, List<Long>> functionFilterMap;

  Map<String, List<Long>> datasetFilterMap;

  Map<String, List<Long>> metricFilterMap;

  Map<String, Map<String, List<Long>>> dimensionFilterMap;

  Map<String, List<Long>> issueTypeFilterMap;

  public SearchFilters() {

  }

  public SearchFilters(Map<String, List<Long>> statusFilterMap, Map<String, List<Long>> functionFilterMap,
      Map<String, List<Long>> datasetFilterMap, Map<String, List<Long>> metricFilterMap,
      Map<String, Map<String, List<Long>>> dimensionFilterMap, Map<String, List<Long>> issueTypeFilterMap) {
    super();
    this.statusFilterMap = sortByValue(statusFilterMap, true);
    this.functionFilterMap = sortByValue(functionFilterMap, true);
    this.datasetFilterMap = sortByValue(datasetFilterMap, true);
    this.metricFilterMap = sortByValue(metricFilterMap, true);
    this.dimensionFilterMap = new TreeMap<>();
    for (String dimensionName : dimensionFilterMap.keySet()) {
      this.dimensionFilterMap.put(dimensionName, sortByValue(dimensionFilterMap.get(dimensionName), true));
    }
    this.issueTypeFilterMap = issueTypeFilterMap;
  }

  public static <K, V extends Comparable<? super V>> Map<K, List<V>> sortByValue(Map<K, List<V>> map) {
    return sortByValue(map, false);
  }

  public static <K, V extends Comparable<? super V>> Map<K, List<V>> sortByValue(Map<K, List<V>> map, final boolean descending) {
    final int sortMultipler = (descending) ? -1 : 1;
    List<Map.Entry<K, List<V>>> list = new LinkedList<Map.Entry<K, List<V>>>(map.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<K, List<V>>>() {
      public int compare(Map.Entry<K, List<V>> o1, Map.Entry<K, List<V>> o2) {
        return sortMultipler * (Integer.valueOf(o1.getValue().size())).compareTo(o2.getValue().size());
      }
    });

    Map<K, List<V>> result = new LinkedHashMap<K, List<V>>();
    for (Map.Entry<K, List<V>> entry : list) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

  public Map<String, List<Long>> getStatusFilterMap() {
    return statusFilterMap;
  }

  public void setStatusFilterMap(Map<String, List<Long>> statusFilterMap) {
    this.statusFilterMap = statusFilterMap;
  }

  public Map<String, List<Long>> getFunctionFilterMap() {
    return functionFilterMap;
  }

  public void setFunctionFilterMap(Map<String, List<Long>> functionFilterMap) {
    this.functionFilterMap = functionFilterMap;
  }

  public Map<String, List<Long>> getDatasetFilterMap() {
    return datasetFilterMap;
  }

  public void setDatasetFilterMap(Map<String, List<Long>> datasetFilterMap) {
    this.datasetFilterMap = datasetFilterMap;
  }

  public Map<String, List<Long>> getMetricFilterMap() {
    return metricFilterMap;
  }

  public void setMetricFilterMap(Map<String, List<Long>> metricFilterMap) {
    this.metricFilterMap = metricFilterMap;
  }

  public Map<String, Map<String, List<Long>>> getDimensionFilterMap() {
    return dimensionFilterMap;
  }

  public void setDimensionFilterMap(Map<String, Map<String, List<Long>>> dimensionFilterMap) {
    this.dimensionFilterMap = dimensionFilterMap;
  }

  public Map<String, List<Long>> getIssueTypeFilterMap() {
    return issueTypeFilterMap;
  }

  public void setIssueTypeFilterMap(Map<String, List<Long>> issueTypeFilterMap) {
    this.issueTypeFilterMap = issueTypeFilterMap;
  }

  public static List<MergedAnomalyResultDTO> applySearchFilters(List<MergedAnomalyResultDTO> anomalies, SearchFilters searchFilters) {
    if (searchFilters == null) {
      return anomalies;
    }
    Map<String, List<Long>> feedbackFilterMap = searchFilters.getStatusFilterMap();
    Map<String, List<Long>> functionFilterMap = searchFilters.getFunctionFilterMap();
    Map<String, List<Long>> datasetFilterMap = searchFilters.getDatasetFilterMap();
    Map<String, List<Long>> metricFilterMap = searchFilters.getMetricFilterMap();
    Map<String, Map<String, List<Long>>> dimensionFilterMap = searchFilters.getDimensionFilterMap();
    Map<String, List<Long>> issueTypeFilterMap = searchFilters.getIssueTypeFilterMap();
    List<MergedAnomalyResultDTO> filteredAnomalies = new ArrayList<>();
    for (MergedAnomalyResultDTO mergedAnomalyResultDTO : anomalies) {
      boolean passed = true;
      // check feedback filter
      AnomalyFeedback feedback = mergedAnomalyResultDTO.getFeedback();
      String status = null;
      if (feedback != null) {
        status = feedback.getFeedbackType().getUserReadableName();
      } else {
        // If feedback is null, assign NO_FEEDBACK to the status
        status = AnomalyFeedbackType.NO_FEEDBACK.getUserReadableName();
      }
      passed = passed && checkFilter(feedbackFilterMap, status);
      // check status filter
      String functionName = "unknown";
      if (mergedAnomalyResultDTO.getFunction() != null) {
        functionName = mergedAnomalyResultDTO.getFunction().getFunctionName();
      }
      if (mergedAnomalyResultDTO.getDetectionConfigId() != null) {
        functionName = String.format("DetectionConfig %d", mergedAnomalyResultDTO.getDetectionConfigId());
      }
      passed = passed && checkFilter(functionFilterMap, functionName);
      // check datasetFilterMap
      String dataset = mergedAnomalyResultDTO.getCollection();
      passed = passed && checkFilter(datasetFilterMap, dataset);
      // check metricFilterMap
      String metric = mergedAnomalyResultDTO.getMetric();
      passed = passed && checkFilter(metricFilterMap, metric);
      // check dimensionFilterMap
      if (dimensionFilterMap != null) {
        DimensionMap dimensions = mergedAnomalyResultDTO.getDimensions();
        for (String dimensionName : dimensions.keySet()) {
          if (dimensionFilterMap.containsKey(dimensionName)) {
            String dimensionValue = dimensions.get(dimensionName);
            passed = passed && checkFilter(dimensionFilterMap.get(dimensionName), dimensionValue);
          }
        }
      }
      // check issue type
      Map<String, String> properties = mergedAnomalyResultDTO.getProperties();
      if (MapUtils.isNotEmpty(properties) && properties.containsKey(MergedAnomalyResultDTO.ISSUE_TYPE_KEY)) {
        String issueType = properties.get(MergedAnomalyResultDTO.ISSUE_TYPE_KEY);
        passed = passed && checkFilter(issueTypeFilterMap, issueType);
      }

      if (passed) {
        filteredAnomalies.add(mergedAnomalyResultDTO);
      }
    }
    return filteredAnomalies;
  }

  private static boolean checkFilter(Map<String, List<Long>> filterMap, String value) {
    if (filterMap != null && !filterMap.isEmpty()) {
      return filterMap.containsKey(value);
    }
    return true;
  }

  public static SearchFilters fromAnomalies(List<MergedAnomalyResultDTO> anomalies) {

    Map<String, List<Long>> feedbackFilterMap = new HashMap<>();
    Map<String, List<Long>> functionFilterMap = new HashMap<>();
    Map<String, List<Long>> datasetFilterMap = new HashMap<>();
    Map<String, List<Long>> metricFilterMap = new HashMap<>();
    Map<String, Map<String, List<Long>>> dimensionFilterMap = new HashMap<>();
    Map<String, List<Long>> issueTypeFilterMap = new HashMap<>();

    for (MergedAnomalyResultDTO mergedAnomalyResultDTO : anomalies) {
      // update feedback filter
      AnomalyFeedback feedback = mergedAnomalyResultDTO.getFeedback();
      String status = null;
      if (feedback != null) {
        status = feedback.getFeedbackType().getUserReadableName();
      } else {
        // If anomaly feedback is null, assign NO_FEEDBACK
        status = AnomalyFeedbackType.NO_FEEDBACK.getUserReadableName();
      }
      update(feedbackFilterMap, status, mergedAnomalyResultDTO.getId());

      // update function filter
      String functionName = "unknown";
      if (mergedAnomalyResultDTO.getFunction() != null) {
        functionName = mergedAnomalyResultDTO.getFunction().getFunctionName();
      }
      if (mergedAnomalyResultDTO.getDetectionConfigId() != null) {
        try {
          functionName = DAORegistry.getInstance()
              .getDetectionConfigManager()
              .findById(mergedAnomalyResultDTO.getDetectionConfigId())
              .getName();
        } catch (Exception e) {
          functionName = String.format("DetectionConfig %d", mergedAnomalyResultDTO.getDetectionConfigId());
        }
      }
      update(functionFilterMap, functionName, mergedAnomalyResultDTO.getId());

      // update datasetFilterMap
      String dataset = mergedAnomalyResultDTO.getCollection();
      update(datasetFilterMap, dataset, mergedAnomalyResultDTO.getId());
      // update metricFilterMap
      String metric = mergedAnomalyResultDTO.getMetric();
      update(metricFilterMap, metric, mergedAnomalyResultDTO.getId());
      // update dimension
      DimensionMap dimensions = mergedAnomalyResultDTO.getDimensions();
      for (String dimensionName : dimensions.keySet()) {
        if (!dimensionFilterMap.containsKey(dimensionName)) {
          dimensionFilterMap.put(dimensionName, new HashMap<String, List<Long>>());
        }
        String dimensionValue = dimensions.get(dimensionName);
        update(dimensionFilterMap.get(dimensionName), dimensionValue, mergedAnomalyResultDTO.getId());
      }
      // update issue type
      Map<String, String> properties = mergedAnomalyResultDTO.getProperties();
      if (MapUtils.isNotEmpty(properties) && properties.containsKey(MergedAnomalyResultDTO.ISSUE_TYPE_KEY)) {
        String issueType = properties.get(MergedAnomalyResultDTO.ISSUE_TYPE_KEY);
        update(issueTypeFilterMap, issueType, mergedAnomalyResultDTO.getId());
      }
    }

    return new SearchFilters(feedbackFilterMap, functionFilterMap, datasetFilterMap, metricFilterMap, dimensionFilterMap,
        issueTypeFilterMap);
  }

  private static void update(Map<String, List<Long>> map, String value, Long anomalyId) {
    List<Long> list = map.get(value);
    if (list == null) {
      map.put(value, new ArrayList<Long>());
    }
    map.get(value).add(anomalyId);

  }

  public static SearchFilters fromJSON(String searchFiltersJSON) {
    try {
      if (searchFiltersJSON != null) {
        return OBJECT_MAPPER.readValue(searchFiltersJSON, SearchFilters.class);
      }
    } catch (Exception e) {
      LOG.warn("Exception while parsing searchFiltersJSON  '%s'", searchFiltersJSON, e);
    }
    return null;
  }

  // @Override
  // public String toString() {
  // return this;
  //// return Objects.toStringHelper(this).add(name, value)
  // }

}
