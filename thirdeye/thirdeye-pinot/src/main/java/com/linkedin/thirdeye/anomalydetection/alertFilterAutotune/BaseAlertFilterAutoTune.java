/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.anomalydetection.alertFilterAutotune;

import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.PrecisionRecallEvaluator;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The abstract class for Alert Filter AutoTune
 * BaseAlertFilterAutoTune initiates the AutoTune class by assigning training and tuning configuration
 * BaseAlertFilterAutoTune is designed to be extended by specific AlertFilterAutoTune class
 */
public abstract class BaseAlertFilterAutoTune implements AlertFilterAutoTune {
  private final static Logger LOG = LoggerFactory.getLogger(BaseAlertFilterAutoTune.class);

  protected AutotuneConfigDTO autotuneConfig;
  protected List<MergedAnomalyResultDTO> trainingAnomalies;

  public void init(List<MergedAnomalyResultDTO> anomalies, AutotuneConfigDTO autotuneConfig) {
    this.trainingAnomalies = anomalies;
    this.autotuneConfig = autotuneConfig;
  }

  public PrecisionRecallEvaluator getEvaluator(AlertFilter alertFilter, List<MergedAnomalyResultDTO> anomalies){
    return new PrecisionRecallEvaluator(alertFilter, anomalies);
  }

  public AutotuneConfigDTO getAutotuneConfig() {
    return this.autotuneConfig;
  }


  public Properties getTuningProperties() {
    return this.autotuneConfig.getTuningProps();
  }

  public void setAutotuneConfig(AutotuneConfigDTO autotuneConfig) {
    this.autotuneConfig = autotuneConfig;
  }

  public void setTuningProperties(Properties tuningProps) {
    this.autotuneConfig.setTuningProps(tuningProps);
  }

  public List<MergedAnomalyResultDTO> getTrainingAnomalies() {
    return this.trainingAnomalies;
  }

  public AlertFilter getAlertFilter() {
    return this.autotuneConfig.getAlertFilter();
  }


}
