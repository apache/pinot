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

package org.apache.pinot.thirdeye.detection.yaml.translator;

import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;


public class DetectionMetricProperties {

  String cron;
  DatasetConfigDTO datasetConfigDTO;
  MetricConfigDTO metricConfigDTO;

  DetectionMetricProperties(String cron, MetricConfigDTO metricConfig, DatasetConfigDTO datasetConfig) {
    this.cron = cron;
    this.metricConfigDTO = metricConfig;
    this.datasetConfigDTO = datasetConfig;
  }

  public String getCron() {
    return cron;
  }

  public void setCron(String cron) {
    this.cron = cron;
  }

  public DatasetConfigDTO getDatasetConfigDTO() {
    return datasetConfigDTO;
  }

  public void setDatasetConfigDTO(DatasetConfigDTO datasetConfigDTO) {
    this.datasetConfigDTO = datasetConfigDTO;
  }

  public MetricConfigDTO getMetricConfigDTO() {
    return metricConfigDTO;
  }

  public void setMetricConfigDTO(MetricConfigDTO metricConfigDTO) {
    this.metricConfigDTO = metricConfigDTO;
  }

}
