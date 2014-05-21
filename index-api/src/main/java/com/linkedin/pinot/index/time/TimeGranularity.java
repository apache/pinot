package com.linkedin.pinot.index.time;

/**
 * TimeGruanularity is used to define the data aggregation level.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 * 
 */

public class TimeGranularity {
  private String _timeGranularity = null;

  public String getTimeGranularity() {
    return _timeGranularity;
  }

  public void setTimeGranularity(String timeGranularity) {
    this._timeGranularity = timeGranularity;
  }
}
