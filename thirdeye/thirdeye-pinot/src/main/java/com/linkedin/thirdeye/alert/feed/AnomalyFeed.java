package com.linkedin.thirdeye.alert.feed;

import com.linkedin.thirdeye.alert.commons.AnomalyFeedConfig;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import java.util.Collection;


public interface AnomalyFeed {
  void init(AlertFilterFactory alertFilterFactory, AnomalyFeedConfig anomalyFeedConfig);

  Collection<MergedAnomalyResultDTO> getAnomalyFeed();

  void updateSnapshot(Collection<MergedAnomalyResultDTO> alertedAnomalies);
}
