package com.linkedin.thirdeye.anomaly.lib.scanstatistics;

/**
 * Simple, fast, online normal statistics object using Welford's algorithm.
 */
public class OnlineNormalStatistics {

  private int _n = 0;
  private double _mean = 0;
  private double _sumSqDiff = 0;

  public OnlineNormalStatistics() {
    // do nothing
  }

  public OnlineNormalStatistics(double[] initialValues) {
    for (double d : initialValues) {
      addValue(d);
    }
  }

  public void addValue(double value) {
    double old_mean = _mean;
    _n++;
    _mean += (value - old_mean) / _n;
    _sumSqDiff += (value - _mean) * (value - old_mean);
  }

  /**
   * @return
   *  A copy of the object.
   */
  public OnlineNormalStatistics copy() {
    OnlineNormalStatistics copy = new OnlineNormalStatistics();
    copy._n = _n;
    copy._mean = _mean;
    copy._sumSqDiff = _sumSqDiff;
    return copy;
  }

  public double getN() {
    return _n;
  }

  public double getMean() {
    return (_n > 0) ? _mean : Double.NaN;
  }

  public double getSumSqDev() {
    return (_n > 0) ? _sumSqDiff : Double.NaN;
  }

  public double getVariance() {
    return (_n > 0) ? _sumSqDiff / (_n - 1) : Double.NaN;
  }

  public double getPopulationVariance() {
    return (_n > 0) ? _sumSqDiff / _n : Double.NaN;
  }

}
