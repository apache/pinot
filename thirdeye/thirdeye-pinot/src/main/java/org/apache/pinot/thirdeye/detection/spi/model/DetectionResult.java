package org.apache.pinot.thirdeye.detection.spi.model;

import java.util.Collections;
import java.util.List;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;


/**
 * The detection result
 */
public class DetectionResult {
  private final List<MergedAnomalyResultDTO> anomalies;
  private final TimeSeries timeseries;

  private DetectionResult(List<MergedAnomalyResultDTO> anomalies, TimeSeries timeseries) {
    this.anomalies = anomalies;
    this.timeseries = timeseries;
  }

  public List<MergedAnomalyResultDTO> getAnomalies() {
    return anomalies;
  }

  public TimeSeries getTimeseries() {
    return timeseries;
  }

  /**
   * Create a empty detection result
   * @return the empty detection result
   */
  public static DetectionResult empty() {
    return new DetectionResult(Collections.emptyList(), TimeSeries.empty());
  }

  /**
   * Create a detection result from a list of anomalies
   * @param anomalies the list of anomalies generated
   * @return the detection result contains the list of anomalies
   */
  public static DetectionResult from(List<MergedAnomalyResultDTO> anomalies) {
    return new DetectionResult(anomalies, TimeSeries.empty());
  }

  /**
   * Create a detection result from a list of anomalies and time series
   * @param anomalies the list of anomalies generated
   * @param timeSeries the time series which including the current, predicted baseline and optionally upper and lower bounds
   * @return the detection result contains the list of anomalies and the time series
   */
  public static DetectionResult from(List<MergedAnomalyResultDTO> anomalies, TimeSeries timeSeries) {
    return new DetectionResult(anomalies, timeSeries);
  }

  @Override
  public String toString() {
    return "DetectionResult{" + "anomalies=" + anomalies + ", timeseries=" + timeseries + '}';
  }
}
