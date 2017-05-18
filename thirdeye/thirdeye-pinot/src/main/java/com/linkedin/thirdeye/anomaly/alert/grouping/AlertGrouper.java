package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;
import java.util.List;
import java.util.Map;

public interface AlertGrouper {
  /**
   * Sets the properties of this grouper.
   *
   * @param props the properties for this grouper.
   */
  void setParameters(Map<String, String> props);

  /**
   * Given a list of anomaly results, this method returns groups of anomaly results such that each group should be
   * sent in the same alert.
   *
   * @return groups of anomaly results.
   */
  Map<DimensionMap, GroupedAnomalyResultsDTO> group(List<MergedAnomalyResultDTO> anomalyResults);
}
