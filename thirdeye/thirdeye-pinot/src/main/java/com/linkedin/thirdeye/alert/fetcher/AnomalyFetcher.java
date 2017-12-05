package com.linkedin.thirdeye.alert.fetcher;

import com.linkedin.thirdeye.alert.commons.AnomalyFetcherConfig;
import com.linkedin.thirdeye.datalayer.dto.AlertSnapshotDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collection;
import java.util.Properties;
import org.joda.time.DateTime;


public interface AnomalyFetcher {
  /**
   * Initialize the AnomalyFetcher with properties
   * @param anomalyFetcherConfig the configuration of the anomaly fetcher
   */
  void init(AnomalyFetcherConfig anomalyFetcherConfig);

  /**
   * Get the alert candidates with the given current date time
   * @param current the current DateTime
   * @param alertSnapShot the snapshot of the alert, containing the time and
   */
  Collection<MergedAnomalyResultDTO> getAlertCandidates(DateTime current, AlertSnapshotDTO alertSnapShot);
}