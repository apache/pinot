package com.linkedin.thirdeye.anomalydetection.alertFilterAutotune;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.AlphaBetaAlertFilter;
import com.linkedin.thirdeye.detector.email.filter.BaseAlertFilter;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by ychung on 2/8/17.
 */
public abstract class BaseAlertFilterAutotune implements AlertFilterAutoTune{
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
  protected Map<String, String> alertFilterProps;

  public void init(AnomalyFunctionDTO functionDTO){
    alertFilterProps = functionDTO.getAlertFilter();
  }
}
