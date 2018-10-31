package com.linkedin.thirdeye.dashboard.views.diffsummary;


public class SummaryGainerLoserResponseRow extends BaseResponseRow {
  public String dimensionName;
  public String dimensionValue;
  public String cost;

  public static SummaryGainerLoserResponseRow buildNotAvailableRow() {
    SummaryGainerLoserResponseRow row = new SummaryGainerLoserResponseRow();
    row.dimensionName = SummaryResponse.NOT_AVAILABLE;
    row.dimensionValue = SummaryResponse.NOT_AVAILABLE;
    row.cost = SummaryResponse.NOT_AVAILABLE;
    row.percentageChange = SummaryResponse.NOT_AVAILABLE;
    row.contributionChange = SummaryResponse.NOT_AVAILABLE;
    row.contributionToOverallChange = SummaryResponse.NOT_AVAILABLE;
    return row;
  }
}
