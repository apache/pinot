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

import java.util.Objects;

public class OnlineDetectionDataBean extends AbstractBean {
  private String dataset;

  private String metric;

  private String onlineDetectionData;

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public String getOnlineDetectionData() {
    return onlineDetectionData;
  }

  public void setOnlineDetectionData(String onlineDetectionData) {
    this.onlineDetectionData = onlineDetectionData;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof OnlineDetectionDataBean)) {
      return false;
    }

    OnlineDetectionDataBean onlineDetectionDataBean = (OnlineDetectionDataBean) o;

    return Objects.equals(getId(), onlineDetectionDataBean.getId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId());
  }
}
