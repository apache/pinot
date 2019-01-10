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

package org.apache.pinot.thirdeye.dashboard.resources.v2.pojo;

import java.util.ArrayList;
import java.util.List;

public class AnomaliesWrapper {

  private List<Long> anomalyIds;
  private SearchFilters searchFilters;
  private List<AnomalyDetails> anomalyDetailsList = new ArrayList<>();
  private int totalAnomalies;
  private int numAnomaliesOnPage;


  public List<Long> getAnomalyIds() {
    return anomalyIds;
  }

  public void setAnomalyIds(List<Long> anomalyIds) {
    this.anomalyIds = anomalyIds;
  }

  public SearchFilters getSearchFilters() {
    return searchFilters;
  }

  public void setSearchFilters(SearchFilters searchFilters) {
    this.searchFilters = searchFilters;
  }

  public List<AnomalyDetails> getAnomalyDetailsList() {
    return anomalyDetailsList;
  }

  public void setAnomalyDetailsList(List<AnomalyDetails> anomalyDetails) {
    this.anomalyDetailsList = anomalyDetails;
  }

  public int getTotalAnomalies() {
    return totalAnomalies;
  }

  public void setTotalAnomalies(int totalAnomalies) {
    this.totalAnomalies = totalAnomalies;
  }

  public int getNumAnomaliesOnPage() {
    return numAnomaliesOnPage;
  }

  public void setNumAnomaliesOnPage(int numAnomaliesOnPage) {
    this.numAnomaliesOnPage = numAnomaliesOnPage;
  }

}
