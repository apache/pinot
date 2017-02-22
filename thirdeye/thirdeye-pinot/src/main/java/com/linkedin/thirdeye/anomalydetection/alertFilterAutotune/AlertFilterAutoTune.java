package com.linkedin.thirdeye.anomalydetection.alertFilterAutotune;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Created by ychung on 2/9/17.
 */
public interface AlertFilterAutoTune {
  // Tune the alert filter properties using labeled anomalies
  // precision and recall need to be improved over old setting
  public Map<String, String> tuneAlertFilter(List<MergedAnomalyResultDTO> anomalyResults, double currentPrecision, double currentRecall) throws Exception;

  // True if model has been updated (improved)
  public boolean isUpdated();

}
