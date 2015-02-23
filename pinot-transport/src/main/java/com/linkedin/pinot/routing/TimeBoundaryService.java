package com.linkedin.pinot.routing;

public interface TimeBoundaryService {

  /**
   * For a given federated resource name, return its time boundary info.
   * 
   * @param resource
   * @return
   */
  TimeBoundaryInfo getTimeBoundaryInfoFor(String resource);

  /**
   * Remove a resource from TimeBoundaryService
   * @param resourceName
   */
  void remove(String resourceName);

  public class TimeBoundaryInfo {
    private String _timeColumn;
    private String _timeValue;

    public String getTimeColumn() {
      return _timeColumn;
    }

    public String getTimeValue() {
      return _timeValue;
    }

    public void setTimeColumn(String timeColumn) {
      _timeColumn = timeColumn;
    }

    public void setTimeValue(String timeValue) {
      _timeValue = timeValue;
    }
  }

}
