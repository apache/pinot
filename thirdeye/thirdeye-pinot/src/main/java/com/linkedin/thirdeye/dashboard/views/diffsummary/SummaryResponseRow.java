package com.linkedin.thirdeye.dashboard.views.diffsummary;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;


/**
 * A POJO for front-end representation.
 */
public class SummaryResponseRow {
  public List<String> names;
  public double baselineValue;
  public double currentValue;
  public String percentageChange;
  public String contributionChange;
  public String contributionToOverallChange;
  public String otherDimensionValues;

  public static SummaryResponseRow buildNotAvailableRow() {
    SummaryResponseRow row = new SummaryResponseRow();
    row.names = new ArrayList<String>();
    row.names.add(SummaryResponse.NOT_AVAILABLE);
    row.percentageChange = SummaryResponse.NOT_AVAILABLE;
    row.contributionChange = SummaryResponse.NOT_AVAILABLE;
    row.contributionToOverallChange = SummaryResponse.NOT_AVAILABLE;
    row.otherDimensionValues = SummaryResponse.NOT_AVAILABLE;
    return row;
  }

  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
  }
}
