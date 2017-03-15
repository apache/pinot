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

  // get initiated alert filter when input has no positive label, use nExpectedAnomalies
  // return the alert filter that can guarantee top nExpectedAnomalies to recommend to clients
  Map<String, String> initiateAutoTune(List<MergedAnomalyResultDTO> anomalyResults, int nExpectedAnomalies) throws Exception;

  // Tune the alert filter properties using labeled anomalies
  // precision and recall need to be improved over old setting
  Map<String, String> tuneAlertFilter(List<MergedAnomalyResultDTO> anomalyResults, double currentPrecision, double currentRecall) throws Exception;

  // True if model has been updated (improved)
  boolean isUpdated();

}
