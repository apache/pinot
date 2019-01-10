/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.anomalydetection.alertFilterAutotune;

import org.apache.pinot.thirdeye.anomaly.detection.lib.AutotuneMethodType;
import org.apache.pinot.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluate;
import org.apache.pinot.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluationMethod;
import org.apache.pinot.thirdeye.datalayer.bao.AutotuneConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.AutotuneConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detector.email.filter.AlertFilter;
import org.apache.pinot.thirdeye.detector.email.filter.BaseAlertFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AlertFilterAutotuneFactory {
  private static Logger LOGGER = LoggerFactory.getLogger(AlertFilterAutotuneFactory.class);
  private final Properties props;

  public AlertFilterAutotuneFactory(String autoTuneConfigPath) {
    props = new Properties();
    try {
      InputStream input = new FileInputStream(autoTuneConfigPath);
      loadPropertiesFromInputStream(input);
    } catch (FileNotFoundException e) {
      LOGGER.error("Alert Filter Property File {} not found", autoTuneConfigPath, e);
    }

  }

  public AlertFilterAutotuneFactory(InputStream input) {
    props = new Properties();
    loadPropertiesFromInputStream(input);
  }

  private void loadPropertiesFromInputStream(InputStream input) {
    try {
      props.load(input);
    } catch (IOException e) {
      LOGGER.error("Error loading the alert filter autotune class from config", e);
    } finally {
      IOUtils.closeQuietly(input);
    }

    LOGGER.info("Found {} entries in alert filter autotune configuration file {}", props.size());
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      LOGGER.info("{}: {}", entry.getKey(), entry.getValue());
    }
  }

  public BaseAlertFilterAutoTune fromSpec(String AutoTuneType, AutotuneConfigDTO autotuneConfigDTO, List<MergedAnomalyResultDTO> anomalies) {
    BaseAlertFilterAutoTune alertFilterAutoTune = new DummyAlertFilterAutoTune();
    if (!props.containsKey(AutoTuneType)) {
      LOGGER.warn("AutoTune from Spec: Unsupported type " + AutoTuneType);
    } else{
      try {
        String className = props.getProperty(AutoTuneType);
        alertFilterAutoTune = (BaseAlertFilterAutoTune) Class.forName(className).newInstance();
        alertFilterAutoTune.init(anomalies, autotuneConfigDTO);
      } catch (Exception e) {
        LOGGER.warn("Failed to init AutoTune from Spec: {}", e.getMessage());
      }
    }
    return alertFilterAutoTune;
  }



}
