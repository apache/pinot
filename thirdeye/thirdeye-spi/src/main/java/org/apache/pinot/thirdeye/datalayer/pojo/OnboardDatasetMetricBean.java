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
 *
 */

package org.apache.pinot.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown=true)
/**
 * This table will be the entry point for users, to add new datasets to Thirdeye or new metrics to datasets in Thirdeye
 * Each data source will keep polling this table, for new entries to onboard
 * The properties will be used to supply things required by the data sources to setup these datasets or metrics
 * Once the entry has been onboarded, we will not check it again
 */
public class OnboardDatasetMetricBean extends AbstractBean {

  private String datasetName;

  private String metricName;

  private String dataSource;

  private Map<String, String> properties;

  private boolean onboarded = false;


  public String getDatasetName() {
    return datasetName;
  }

  public void setDatasetName(String datasetName) {
    this.datasetName = datasetName;
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public String getDataSource() {
    return dataSource;
  }

  public void setDataSource(String dataSource) {
    this.dataSource = dataSource;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }


  public boolean isOnboarded() {
    return onboarded;
  }

  public void setOnboarded(boolean onboarded) {
    this.onboarded = onboarded;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof OnboardDatasetMetricBean)) {
      return false;
    }
    OnboardDatasetMetricBean oc = (OnboardDatasetMetricBean) o;
    return Objects.equals(getId(), oc.getId())
        && Objects.equals(datasetName, oc.getDatasetName())
        && Objects.equals(metricName, oc.getMetricName())
        && Objects.equals(dataSource, oc.getDataSource())
        && Objects.equals(properties, oc.getProperties())
        && Objects.equals(onboarded, oc.isOnboarded());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), datasetName, metricName, dataSource, properties, onboarded);
  }

}
