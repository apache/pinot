package com.linkedin.thirdeye.dashboard.views.diffsummary;

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

  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
  }
}
