package com.linkedin.thirdeye.detector.function;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.List;
import org.joda.time.DateTime;


/**
 * A anomaly function that is used to show week-over-week current and baseline values.
 * This class is used as a backup function when dashboard is unable to construct the corresponding anomaly function
 * for showing data.
 */
public class PresentationalAnomalyFunction extends BaseAnomalyFunction {

  @Override
  public List<RawAnomalyResultDTO> analyze(DimensionMap exploredDimensions, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<RawAnomalyResultDTO> knownAnomalies)
      throws Exception {
    return null;
  }
}
