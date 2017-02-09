package com.linkedin.thirdeye.anomalydetection.alertFilterAutotune;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;
import java.util.Properties;


/**
 * Created by ychung on 2/9/17.
 */
public interface AlertFilterAutoTune {
  // Tune the alert filter properties using labeled anomalies
  // precision and recall need to be improved over old setting
  Properties tuneAlertFilter(List<MergedAnomalyResultDTO> anomalyResults, Properties currentAlertFilterProperties) throws Exception;

  // True if model has been updated (improved)
  boolean isUpdated();
}
