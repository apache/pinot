package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

public class SearchFilters {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(SearchFilters.class);

  Map<String, List<Long>> statusFilterMap;

  Map<String, List<Long>> functionFilterMap;

  Map<String, List<Long>> datasetFilterMap;

  Map<String, List<Long>> metricFilterMap;

  Map<String, Map<String, List<Long>>> dimensionFilterMap;

  public SearchFilters() {

  }

  public SearchFilters(Map<String, List<Long>> statusFilterMap, Map<String, List<Long>> functionFilterMap, Map<String, List<Long>> datasetFilterMap,
      Map<String, List<Long>> metricFilterMap, Map<String, Map<String, List<Long>>> dimensionFilterMap) {
    super();
    this.statusFilterMap = sortByValue(statusFilterMap, true);
    this.functionFilterMap = sortByValue(functionFilterMap, true);
    this.datasetFilterMap = sortByValue(datasetFilterMap, true);
    this.metricFilterMap = sortByValue(metricFilterMap, true);
    this.dimensionFilterMap = new TreeMap<>();
    for (String dimensionName : dimensionFilterMap.keySet()) {
      this.dimensionFilterMap.put(dimensionName, sortByValue(dimensionFilterMap.get(dimensionName), true));
    }
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

  public static List<MergedAnomalyResultDTO> applySearchFilters(List<MergedAnomalyResultDTO> anomalies, SearchFilters searchFilters) {
    if (searchFilters == null) {
      return anomalies;
    }
    Map<String, List<Long>> statusFilterMap = searchFilters.getStatusFilterMap();
    Map<String, List<Long>> functionFilterMap = searchFilters.getFunctionFilterMap();
    Map<String, List<Long>> datasetFilterMap = searchFilters.getDatasetFilterMap();
    Map<String, List<Long>> metricFilterMap = searchFilters.getMetricFilterMap();
    Map<String, Map<String, List<Long>>> dimensionFilterMap = searchFilters.getDimensionFilterMap();
    List<MergedAnomalyResultDTO> filteredAnomalies = new ArrayList<>();
    for (MergedAnomalyResultDTO mergedAnomalyResultDTO : anomalies) {
      boolean passed = true;
      // check status filter
      AnomalyFeedback feedback = mergedAnomalyResultDTO.getFeedback();
      if (feedback != null) {
        String status = feedback.getStatus().toString();
        passed = passed && checkFilter(statusFilterMap, status);
      }
      // check status filter
      String functionName = mergedAnomalyResultDTO.getFunction().getFunctionName();
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

    Map<String, List<Long>> statusFilterMap = new HashMap<>();
    Map<String, List<Long>> functionFilterMap = new HashMap<>();
    Map<String, List<Long>> datasetFilterMap = new HashMap<>();
    Map<String, List<Long>> metricFilterMap = new HashMap<>();
    Map<String, Map<String, List<Long>>> dimensionFilterMap = new HashMap<>();

    for (MergedAnomalyResultDTO mergedAnomalyResultDTO : anomalies) {
      // update status filter
      AnomalyFeedback feedback = mergedAnomalyResultDTO.getFeedback();
      if (feedback != null) {
        String status = feedback.getStatus().toString();
        update(statusFilterMap, status, mergedAnomalyResultDTO.getId());
      }
      // update status filter
      String functionName = mergedAnomalyResultDTO.getFunction().getFunctionName();
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
    }

    return new SearchFilters(statusFilterMap, functionFilterMap, datasetFilterMap, metricFilterMap, dimensionFilterMap);
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
