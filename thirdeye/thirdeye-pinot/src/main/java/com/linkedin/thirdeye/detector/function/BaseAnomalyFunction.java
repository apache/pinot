package com.linkedin.thirdeye.detector.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseAnomalyFunction implements AnomalyFunction {

  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());

  private AnomalyFunctionDTO spec;

  @Override
  public void init(AnomalyFunctionDTO spec) throws Exception {
    this.spec = spec;
  }

  @Override
  public AnomalyFunctionDTO getSpec() {
    return spec;
  }

  protected Properties getProperties() throws IOException {
    Properties props = new Properties();
    if (spec.getProperties() != null) {
      String[] tokens = spec.getProperties().split(";");
      for (String token : tokens) {
        props.load(new ByteArrayInputStream(token.getBytes()));
      }
    }
    return props;
  }

  /**
   * Returns unit change from baseline value
   * @param currentValue
   * @param baselineValue
   * @return
   */
  protected double calculateChange(double currentValue, double baselineValue) {
    return (currentValue - baselineValue) / baselineValue;
  }

  /**
   * Useful when multiple time intervals are needed for fetching current vs baseline data
   *
   * @param monitoringWindowStartTime inclusive
   * @param monitoringWindowEndTime exclusive
   *
   * @return
   */
  public List<Pair<Long, Long>> getDataRangeIntervals(Long monitoringWindowStartTime,
      Long monitoringWindowEndTime) {
    List<Pair<Long, Long>> startEndTimeIntervals = new ArrayList<>();
    startEndTimeIntervals.add(new Pair<>(monitoringWindowStartTime, monitoringWindowEndTime));

    long baselineOffsetMillis = TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);
    startEndTimeIntervals.add(
        new Pair<>(monitoringWindowStartTime - baselineOffsetMillis, monitoringWindowEndTime - baselineOffsetMillis));

    return startEndTimeIntervals;
  }
}
