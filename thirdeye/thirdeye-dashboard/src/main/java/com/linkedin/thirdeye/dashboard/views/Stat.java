package com.linkedin.thirdeye.dashboard.views;

public enum Stat
{
  BASELINE_VALUE,         //0
  CURRENT_VALUE,          //1
  BASELINE_CDF_VALUE,     //2
  CURRENT_CDF_VALUE,      //3
  BASELINE_TOTAL,         //4
  CURRENT_TOTAL,          //5
  BASELINE_RATIO,         //6
  CURRENT_RATIO,          //7
  CONTRIBUTION_DIFFERENCE,//8
  VOLUME_DIFFERENCE,      //9
  SNAPSHOT_CATEGORY,      //10
  DELTA_PERCENT_CHANGE,   //11
  DELTA_ABSOLUTE_CHANGE;   //12
  
  public String toString() {
    return this.name().toLowerCase();
  };
  
}
