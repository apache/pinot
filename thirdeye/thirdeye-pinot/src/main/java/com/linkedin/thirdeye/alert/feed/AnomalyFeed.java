package com.linkedin.thirdeye.alert.feed;

import com.linkedin.thirdeye.alert.commons.AnomalyFeedConfig;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import java.util.Collection;

/**
 * AnomalyFeed aggregates the output from AnomalyFetchers, and filter the anomalies based on user-input criteria.
 * This anomaly feed can simply union the output, or filter then ongoing anomalies if they are less severe than before.
 */
public interface AnomalyFeed {
  /**
   * Initialize the anomaly feed
   * @param alertFilterFactory
   * @param anomalyFeedConfig
   */
  void init(AlertFilterFactory alertFilterFactory, AnomalyFeedConfig anomalyFeedConfig);

  /**
   * Get the aggregated anomalies
   * @return
   */
  Collection<MergedAnomalyResultDTO> getAnomalyFeed();

  /**
   * Update the snapshots by the alerted anomalies
   * @param alertedAnomalies
   */
  void updateSnapshot(Collection<MergedAnomalyResultDTO> alertedAnomalies);
}
