package com.linkedin.thirdeye.anomaly.merge;

import com.linkedin.thirdeye.api.dto.GroupByKey;
import com.linkedin.thirdeye.api.dto.GroupByRow;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient;
import com.linkedin.thirdeye.db.dao.AnomalyMergedResultDAO;
import com.linkedin.thirdeye.db.dao.AnomalyResultDAO;
import com.linkedin.thirdeye.db.entity.AnomalyMergedResult;
import com.linkedin.thirdeye.db.entity.AnomalyResult;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * finds raw anomalies grouped by a strategy and merges them with an existing (in the same group) or
 * new {@link com.linkedin.thirdeye.db.entity.AnomalyMergedResult}
 */
public class AnomalyMergeExecutor {
  AnomalyMergedResultDAO mergedResultDAO;
  AnomalyResultDAO anomalyResultDAO;
  AnomalyMergeConfig mergeConfig;
  PinotThirdEyeClient pinotThirdEyeClient;

  public AnomalyMergeExecutor(AnomalyMergedResultDAO mergedResultDAO,
      AnomalyResultDAO anomalyResultDAO, AnomalyMergeConfig mergeConfig,
      PinotThirdEyeClient pinotThirdEyeClient) {
    this.mergedResultDAO = mergedResultDAO;
    this.anomalyResultDAO = anomalyResultDAO;
    this.mergeConfig = mergeConfig;
    this.pinotThirdEyeClient = pinotThirdEyeClient;
  }

  public void updateMergedResults() {
    // TODO: based on merge strategy, find unprocessed raw results and update mergedResults
    /**
     * Step 1: find all groups of raw (unprocessed) anomalies based on merge strategy (Collection + metric + dimensions)
     *
     * Step 2: For each such group, find the base mergedAnomaly
     *
     * Step 3: call time based merge
     *
     * Step 4: Recompute anomaly score / weight
     */

    Set<GroupByKey> uniqueCategories = new HashSet<>();
    Map<GroupByKey, List<AnomalyResult>> anomalyResultMap = new HashMap<>();
    Map<GroupByKey, AnomalyMergedResult> mergedAnomalyMap = new HashMap<>();

    switch (mergeConfig.getMergeStrategy()) {
    case COLLECTION_METRIC_DIMENSIONS:
      fillDataForCollectionMetricDimensionGroup(uniqueCategories, anomalyResultMap, mergedAnomalyMap);
      break;
    default:
      throw new IllegalArgumentException(
          "Merge strategy " + mergeConfig.getMergeStrategy() + " not supported");
    }

  }

  void fillDataForCollectionMetricDimensionGroup(Set<GroupByKey> uniqueCategories,
      Map<GroupByKey, List<AnomalyResult>> anomalyResultMap,
      Map<GroupByKey, AnomalyMergedResult> mergedAnomalyMap) {
    List<GroupByRow<GroupByKey, Long>> groupByRows = anomalyResultDAO
        .getCountByCollectionMetricDimension(mergeConfig.getStartTime(), mergeConfig.getEndTime());
    for (GroupByRow<GroupByKey, Long> row : groupByRows) {
      uniqueCategories.add(row.getGroupBy());
      // TODO: // FIXME: 8/9/16
    }
  }
}
