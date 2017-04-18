package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;
import java.util.Map;

public interface AlertGrouper<T> {
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
  Map<GroupKey<T>, GroupedAnomalyResults> group(List<MergedAnomalyResultDTO> anomalyResults);

  /**
   * The additional recipients string for this group of anomalies.
   *
   * @param groupKey the key of the group
   *
   * @return the additional recipients for the given group.
   */
  String groupEmailRecipients(GroupKey<T> groupKey);

  /**
   * Constructs group key from the given raw key and based on the setting of this grouper.
   *
   * @param rawKey the information to construct a group key of this grouper.
   *
   * @return a group key.
   */
  GroupKey<T> constructGroupKey(T rawKey);
}
