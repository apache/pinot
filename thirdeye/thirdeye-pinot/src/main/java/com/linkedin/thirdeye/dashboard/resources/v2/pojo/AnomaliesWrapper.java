package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

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
