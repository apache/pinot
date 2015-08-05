package com.linkedin.thirdeye.reporting.api;

public class GroupBy {

  String startTime;
  String dimension;



  public GroupBy(String startTime, String dimension) {
    this.startTime = startTime;
    this.dimension = dimension;
  }


  public String getStartTime() {
    return startTime;
  }
  public void setStartTime(String startTime) {
    this.startTime = startTime;
  }
  public String getDimension() {
    return dimension;
  }
  public void setDimension(String dimension) {
    this.dimension = dimension;
  }



}
